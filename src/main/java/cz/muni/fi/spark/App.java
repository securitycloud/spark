package cz.muni.fi.spark;

import cz.muni.fi.commons.MapValueComparator;
import cz.muni.fi.commons.Triplet;
import cz.muni.fi.kafka.OutputProducer;
import cz.muni.fi.spark.accumulators.MapAccumulator;
import cz.muni.fi.spark.tests.*;
import cz.muni.fi.util.PropertiesParser;

import java.io.FileNotFoundException;
import java.io.PrintWriter;

import kafka.serializer.StringDecoder;
import kafka.utils.ZkUtils;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;


/**
 * Class to be submitted to spark-submit.
 */
public class App {

    static final long SPARK_STREAMING_BATCH_INTERVAL = 1000;
    static final int TEST_DATA_RECORDS_PER_PARTITION = 100_000;
    static final int TEST_DATA_PARTITIONS_COUNT = 100;
    //static final int TEST_DATA_RECORDS_SIZE = 200_000_000; // total amount of events in our data set, indicates test end
    static final String FILTER_TEST_IP = "141.57.244.116";

    private static final OutputProducer prod = new OutputProducer();

    /**
     * Main function executed on master node via /bin/spark-submit. Prepares and runs desired test.
     *
     * @param args first argument needs to be the name of the test class, second the number of machines its run on
     */
    public static void main(String[] args) throws MissingArgumentException {
        // VALIDATE AND PARSE COMMAND LINE ARGUMENTS
        if (args.length != 2) {
            throw new IllegalArgumentException("wrong number of arguments, needs to be 2, is: " + args.length);
        }

        String testClass; // name of class to be submitted
        try {
            testClass = args[0];
        } catch (ArrayIndexOutOfBoundsException ex) {
            throw new MissingArgumentException("missing argument: 'testClass'");
        }
        final int machinesCount; // total count of machines including master
        final int kafkaStreamsCount; // number of kafka streams, should be less or equal to the number of kafka partitions
        try {
            machinesCount = Integer.parseInt(args[1]);
            if (machinesCount < 3) {
                throw new IllegalArgumentException("argument with number of machines needs to be greater or equal 3, is: " + args[1]);
            }
            kafkaStreamsCount = 20;
            // Optimization: streamsCount = (machinesCount/2)? check how many executors are working and if the input rate of Kafka gets processed fast enough
        } catch (ArrayIndexOutOfBoundsException ex) {
            throw new MissingArgumentException("missing argument: 'machinesCount'");
        } catch (NumberFormatException numberFormatException) {
            throw new IllegalArgumentException("argument with number of machines needs to be a number");
        }
        System.out.println("Started test: '" + testClass + "' on " + machinesCount + " machines with " + kafkaStreamsCount + " kafka streams.");

        final int TEST_DATA_RECORDS_SIZE = TEST_DATA_RECORDS_PER_PARTITION * TEST_DATA_PARTITIONS_COUNT;

        // INITIALIZE SPARK CONFIGURATION AND KAFKA PROPERTIES
        final SparkConf sparkConf = getSparkConf();
        final Properties kafkaProps = PropertiesParser.getKafkaProperties();
        final Properties applicationProps = PropertiesParser.getApplicationProperties();
        final JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.milliseconds(SPARK_STREAMING_BATCH_INTERVAL));

        // INITIALIZE SPARK KAFKA STREAMS
        Map<String, Integer> topicMap = new HashMap<>(); // consumer topic map
        topicMap.put(kafkaProps.getProperty("consumer.topic"), 1);
        //topicMap.put(kafkaProps.getProperty("consumer.topic") + "-" + (machinesCount) + "part", 1); // topic, numThreads

        Map<String, String> kafkaPropsMap = new HashMap<>(); // consumer properties
        for (String key : kafkaProps.stringPropertyNames()) {
            kafkaPropsMap.put(key, kafkaProps.getProperty(key));
        }

        // reset zookeeper data for group so all messages from topic beginning can be read
        ZkUtils.maybeDeletePath(kafkaProps.getProperty("zookeeper.url"), "/consumers/" + kafkaProps.getProperty("group.id"));

        List<JavaPairDStream<String, String>> kafkaStreams = new ArrayList<>(kafkaStreamsCount);
        for (int i = 0; i < kafkaStreamsCount; i++) {
            // standard basic stream creation
            // kafkaStreams.add(KafkaUtils.createStream(jssc, kafkaProps.getProperty("zookeeper.url"), "1", topicMap));

            // advanced stream creation with kafka properties as parameter
            kafkaStreams.add(KafkaUtils.createStream(jssc, String.class, String.class, StringDecoder.class,
                    StringDecoder.class, kafkaPropsMap, topicMap, StorageLevel.MEMORY_AND_DISK_SER()));

            // direct stream - new approach test
            //kafkaStreams.add(KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaPropsMap, topicSet));
        }

        // INITIALIZE SPARK STREAMING AND PREPARE SHARED VARIABLES IN ALL TESTS
        JavaPairDStream<String, String> messages = jssc.union(kafkaStreams.get(0), kafkaStreams.subList(1, kafkaStreams.size()));
        Accumulator<Integer> processedRecordsCounter = jssc.sparkContext().accumulator(0); // accumulator used for performance monitoring in all tests
        Accumulator<Map<String, Integer>> ipPackets = jssc.sparkContext().accumulator(new HashMap<>(), new MapAccumulator()); // Aggregation/TopN/SynScan
        Accumulator<Integer> filteredIpCount = jssc.sparkContext().accumulator(0); // FilterIPTest specific
        Accumulator<Map<String, Integer>> ipOccurrences = jssc.sparkContext().accumulator(new HashMap<>(), new MapAccumulator());

        // START STREAMING WITH PROVIDED TEST CLASS AS ARGUMENT
        switch (testClass) {
            case "ReadWriteTest": {
                messages.foreachRDD(new ReadWriteTest(processedRecordsCounter));
                break;
            }
            case "FilterIPTest": {
                messages.foreachRDD(new FilterIPTest(FILTER_TEST_IP, processedRecordsCounter, filteredIpCount));
                break;
            }
            case "CountTest": {
                messages.foreachRDD(new CountTest(FILTER_TEST_IP, processedRecordsCounter, filteredIpCount));
                break;
            }
            case "AggregationTest": {
                messages.foreachRDD(new AggregationTest(processedRecordsCounter, ipPackets));
                break;
            }
            case "TopNTest": {
                messages.foreachRDD(new AggregationTest(processedRecordsCounter, ipPackets));
                break;
            }
            case "SynScanTest": {
                messages.foreachRDD(new SynScanTest(processedRecordsCounter, ipOccurrences));
                break;
            }
            case "Undefined": {
                throw new IllegalArgumentException("test class name was not properly passed as argument");
            }
            default: {
                throw new IllegalArgumentException("test class name does not exist: " + testClass);
            }
        }

        // REGULAR CHECK FOR DATA SET FINISH AND TEST POST PROCESSING, PERFORMANCE MEASUREMENT
        new Thread(() -> {
            final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            Long min = 0L;
            Long max = 0L;
            LocalDateTime startDateTime = LocalDateTime.now();
            boolean finished = false;
            boolean receivedData = false;
            int step = 0; // every 5 s (100th call) measurements are printed
            while (!finished) {
                Integer processedRecords = processedRecordsCounter.value();
                if (!receivedData && processedRecords > 0) {
                    receivedData = true;
                    startDateTime = LocalDateTime.now().minusSeconds(SPARK_STREAMING_BATCH_INTERVAL / 1000);
                }
                if (processedRecords >= TEST_DATA_RECORDS_SIZE) {
                    final String resultsTopic = applicationProps.getProperty("application.resultsTopic");
                    final String testInfo = LocalDateTime.now().format(formatter) + " " + testClass + " [" +
                            machinesCount + " machines / " + kafkaStreamsCount + " streams]";
                    
                    switch (testClass) {
                        case "ReadWriteTest": {
                            final String result = "Amount of flows: " + processedRecords;
                            // kafka consumer total sum of flows
                            prod.send(new Tuple2<>(null, result));
                            break;
                        }
                        case "FilterIPTest": {
                            final String result = "IP: " + FILTER_TEST_IP + ", amount of flows: " + filteredIpCount.value() +
                                    ", filter ratio: " + ((float)filteredIpCount.value() / processedRecords);
                            // kafka consumer gets a message with IP and the sum of flows
                            prod.send(new Tuple2<>(null, result));
                            break;
                        }
                        case "CountTest": {
                            final String result = "IP: " + FILTER_TEST_IP + ", amount of packets: " + filteredIpCount.value();
                            // kafka consumer gets a message with IP and the sum of packets
                            prod.send(new Tuple2<>(null, result));
                            break;
                        }
                        case "AggregationTest": {
                            final String result = "IP: " + FILTER_TEST_IP + ", amount of packets: " + ipPackets.value().get(FILTER_TEST_IP);
                            // kafka consumer gets a message with IP and the sum of packets
                            prod.send(new Tuple2<>(null, result));
                            break;
                        }
                        case "TopNTest": {
                            Map<String, Integer> total = ipPackets.value();
                            // sort map by values
                            MapValueComparator valueComparator = new MapValueComparator(total);
                            TreeMap<String, Integer> sortedTotal = new TreeMap<>(valueComparator);
                            sortedTotal.putAll(total);
                            // filter out n IP results with highest sum of packets
                            int n = 10; // top n
                            int position = 1; // starting index
                            List<Triplet<Integer, String, Integer>> topElements = new ArrayList<>();
                            for (String ip : sortedTotal.keySet()) {
                                // triplet of position, ip and packet count
                                Triplet<Integer, String, Integer> triplet = new Triplet<>(position, ip, total.get(ip));
                                topElements.add(triplet);
                                if (position == n) {
                                    break;
                                } else {
                                    position++;
                                }
                            }
                            // kafka consumer gets a list of triplets with position, ip and packet count
                            prod.send(new Tuple2<>(null, topElements.toString()));
                            break;
                        }
                        case "SynScanTest": {
                            Map<String, Integer> total = ipOccurrences.value();
                            //Map<String, Integer> filtered = SynScanTest.filterMap(total, 10);

                            MapValueComparator valueComparator = new MapValueComparator(total);
                            TreeMap<String, Integer> sorted = new TreeMap<>(valueComparator);
                            sorted.putAll(total);

                            int n = 100; // top n
                            int position = 1; // starting index
                            List<Triplet<Integer, String, Integer>> topElements = new ArrayList<>();
                            for (String ip : sorted.keySet()) {
                                // triplet of position, ip and packet count
                                Triplet<Integer, String, Integer> triplet = new Triplet<>(position, ip, total.get(ip));
                                topElements.add(triplet);
                                if (position == n) {
                                    break;
                                } else {
                                    position++;
                                }
                            }
                            // kafka consumer gets a list of triplets with position, ip and packet count
                            prod.send(new Tuple2<>(null, topElements.toString()));
                            break;
                        }
                        default: {
                            break;
                        }
                    }
                    
                    // common for all tests: print test info to console and test info + performance info to kafka
                    Long processingTimeInMillis = ChronoUnit.MILLIS.between(startDateTime, LocalDateTime.now());
                    Long averageSpeed = (processedRecords / (processingTimeInMillis / 1000));
                    final String performanceResult = String.format("[k flows/s (min/max/avg): %s | %s | %s ] [processed total: %s]",
                            min/1000, max/1000, averageSpeed/1000, processedRecords);
                    System.out.println(testInfo);
                    System.out.println(performanceResult);
                    printTestResult(resultsTopic, null, Arrays.asList(testInfo + " " + performanceResult));
                    
                    finished = true;
                } else {
                    // Updates min and max processed records rate with average that is taken every 5 seconds if test is still running
                    if (processedRecords > 0 && (step % 100) == 0) {
                        Long processingTimeInMillis = ChronoUnit.MILLIS.between(startDateTime, LocalDateTime.now());
                        if (processingTimeInMillis >= 10000) { // give spark some time to start processing records
                            Long averageSpeed = (processedRecords / (processingTimeInMillis / 1000));
                            if (min == 0L) {
                                min = averageSpeed;
                            }
                            if (averageSpeed > max) {
                                max = averageSpeed;
                            }
                            if (averageSpeed < min) {
                                min = averageSpeed;
                            }
                            System.out.println(String.format("[k flows/s (min/max/avg): %s | %s | %s ] [processed total: %s]",
                                    min/1000, max/1000, averageSpeed/1000, processedRecords));
                        }
                    }
                    step++;
                }
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    System.out.println(e.getLocalizedMessage());
                }
            }
        }).start();

        jssc.start();
        jssc.awaitTermination();
    }

    /**
     * Save test results to a file or send to kafka according to passed parameters
     * 
     * @param kafkaTopic name of Kafka topic
     * @param filename filename to be written to
     * @param resultLines list of lines with results to be saved
     */
    private static void printTestResult(String kafkaTopic, String filename, List<String> resultLines) {
        if (filename != null) {
            try {
                PrintWriter pw = new PrintWriter(filename);
                for (String resultLine : resultLines) {
                    pw.append(resultLine + "\r\n");
                }
                pw.close();
            } catch (FileNotFoundException ex) {

            }
        }
        
        if (kafkaTopic != null) {
            for (String resultLine : resultLines) {
                prod.send(new Tuple2<>(null, resultLine), kafkaTopic);
            }
        }
    }

    /**
     * Takes configuration from pom.xml translated into spark.properties as SparkConf.
     *
     * @return SparkConf configuration for spark context
     */
    public static SparkConf getSparkConf() {
        final Properties sparkProps = PropertiesParser.getSparkProperties();
        return new SparkConf()
                .setSparkHome(sparkProps.getProperty("spark.home"))
                .setAppName(sparkProps.getProperty("spark.app.name"))
                .setMaster(sparkProps.getProperty("spark.master.url"))
                .set("spark.executor.memory", sparkProps.getProperty("spark.executor.memory"))
                .set("spark.serializer", sparkProps.getProperty("spark.serializer"))
                .set("spark.driver.cores", sparkProps.getProperty("spark.driver.cores"))
                .set("spark.default.parallelism", sparkProps.getProperty("spark.default.parallelism"));
    }
}
