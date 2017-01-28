package cz.muni.fi.spark;

import cz.muni.fi.commons.LongTriplet;
import cz.muni.fi.commons.MapValueComparator;
import cz.muni.fi.commons.Triplet;
import cz.muni.fi.kafka.OutputProducer;
import cz.muni.fi.spark.accumulators.BasicStatisticsAccumulators;
import cz.muni.fi.spark.accumulators.MapAccumulator;
import cz.muni.fi.spark.statistics.BasicStatistics;
import cz.muni.fi.spark.tests.*;
import cz.muni.fi.util.PropertiesParser;
import cz.muni.fi.util.Utils;

import java.io.FileNotFoundException;
import java.io.PrintWriter;

import kafka.serializer.StringDecoder;
import kafka.utils.ZkUtils;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.util.LongAccumulator;
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
    static final int TEST_DATA_RECORDS_SIZE = 2000000; // total amount of events in our data set, indicates test end
    static final String FILTER_TEST_IP = "62.148.241.49"; // used for FilterIPTest only

    private static final OutputProducer prod = new OutputProducer();
    
    private static BasicStatisticsAccumulators statAccums;
    private static boolean firstStatistics = true;

    /**
     * Main function executed on master node via /bin/spark-submit. Prepares and runs desired test.
     *
     * @param args first argument needs to be the name of the test class, second the number of machines its run on
     */
    public static void main(String[] args) throws MissingArgumentException, InterruptedException {
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
        final int machinesCount = 1; // total count of machines including master
        final int kafkaStreamsCount = 1; // number of kafka streams, should be less or equal to the number of kafka partitions

        System.out.println("Started test: '" + testClass + "' on " + machinesCount + " machines with " + kafkaStreamsCount + " kafka streams.");

        // INITIALIZE SPARK CONFIGURATION AND KAFKA PROPERTIES
        final SparkConf sparkConf = getSparkConf();
        final Properties kafkaProps = PropertiesParser.getKafkaProperties();
        final Properties applicationProps = PropertiesParser.getApplicationProperties();
        final JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.milliseconds(SPARK_STREAMING_BATCH_INTERVAL));

        // INITIALIZE SPARK KAFKA STREAMS
        Map<String, Integer> topicMap = new HashMap<>(); // consumer topic map
        topicMap.put(kafkaProps.getProperty("consumer.topic"), 1); // topic, numThreads

        Map<String, String> kafkaPropsMap = new HashMap<>(); // consumer properties
        for (String key : kafkaProps.stringPropertyNames()) {
            kafkaPropsMap.put(key, kafkaProps.getProperty(key));
        }

        // reset zookeeper data for group so all messages from topic beginning can be read
        ZkUtils.maybeDeletePath(kafkaProps.getProperty("zookeeper.url"), "/consumers/" + kafkaProps.getProperty("group.id"));

        // INITIALIZE SPARK STREAMING AND PREPARE SHARED VARIABLES IN ALL TESTS
        JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(
                jssc, String.class, String.class, StringDecoder.class, StringDecoder.class,
                kafkaPropsMap, topicMap, StorageLevel.MEMORY_AND_DISK_SER());
        LongAccumulator processedRecordsCounter = jssc.ssc().sparkContext().longAccumulator("processedRecordsCounter");
        LongAccumulator filteredIpCount = jssc.ssc().sparkContext().longAccumulator("filteredIpCount"); // FilterIPTest specific
        Accumulator<Map<String, Integer>> ipPackets = jssc.sparkContext().accumulator(new HashMap<>(), new MapAccumulator()); // Aggregation/TopN/SynScan
        Accumulator<Map<String, Integer>> ipOccurrences = jssc.sparkContext().accumulator(new HashMap<>(), new MapAccumulator());
        statAccums = new BasicStatisticsAccumulators(jssc);

        // START STREAMING WITH PROVIDED TEST CLASS AS ARGUMENT
        switch (testClass) {
            case "ReadWriteTest": {
                messages.foreachRDD(new ReadWriteTest(processedRecordsCounter));
                break;
            }
            case "FilterIPTest": {
                messages.foreachRDD(new FilterIPTest(FILTER_TEST_IP, processedRecordsCounter));
                break;
            }
            case "CountTest": {
                messages.foreachRDD(new CountTest(processedRecordsCounter, filteredIpCount));
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
            case "Statistics": {
                messages.foreachRDD(new BasicStatistics(processedRecordsCounter, statAccums));
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
            int step = 0; // on 10 measurements are printed
            while (!finished) {
                Long processedRecords = processedRecordsCounter.value();
                if (processedRecords >= TEST_DATA_RECORDS_SIZE) {
                    final String resultsTopic = applicationProps.getProperty("kafka.producer.resultsTopic");
                    final String testInfo = LocalDateTime.now().format(formatter) + " " + testClass + " [" +
                            machinesCount + " machines / " + kafkaStreamsCount + " streams]";
                    
                    switch (testClass) {
                        case "ReadWriteTest": {
                            break;
                        }
                        case "FilterIPTest": {
                            break;
                        }
                        case "CountTest": {
                            final String result = "IP: " + FILTER_TEST_IP + ", amount of packets: " + filteredIpCount.value();
                            // kafka consumer gets a message with IP and the sum of packets
                            prod.send(new Tuple2<>(null, result));
                            break;
                        }
                        case "AggregationTest": {
                            // kafka consumer gets a map with dst IPs and the sums of packets
                            ipPackets.value().forEach((k, v) -> prod.send(new Tuple2<>(null, k + ":" + v)));
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
                            Map<String, Integer> filtered = SynScanTest.filterMap(ipOccurrences.value(), 10);
                            filtered.forEach((k, v) -> prod.send(new Tuple2<>(null, k + ":" + v)));
                            break;
                        }
                        default: {
                            prod.send(new Tuple2<>(null, testClass + ": Done"));
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
                    if (processedRecords > 0 && (step % 10) == 0) {
                        Long processingTimeInMillis = ChronoUnit.MILLIS.between(startDateTime, LocalDateTime.now());
//                        if (processingTimeInMillis >= 10000) { // give spark some time to start processing records
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
//                        }
                    }
                    
                    if (testClass.equals("Statistics") && processedRecords > 0 && (step % 2) == 0) {
                        final String statisticsTopic = applicationProps.getProperty("kafka.producer.statisticsTopic");
                        final String statisticsFormat = applicationProps.getProperty("kafka.producer.statisticsFormat");
                        
                        // get current accumulator values
                        LongTriplet totalValue = statAccums.getTotalCounter().value();
                        Map<String, LongTriplet> portCounter = statAccums.getPortCounter().value();
                        SortedMap<String, LongTriplet> portSortedMap = Utils.getSortedMapWithTripletComparator(portCounter);
                        SortedMap<String, LongTriplet> ipSortedMap = Utils.getSortedMapWithTripletComparator(statAccums.getIpCounter().value());
                        SortedMap<String, LongTriplet> ipAggregationSortedMap = Utils.getSortedMapWithTripletComparator(statAccums.getIpAggregationCounter().value());
                        
                        // reset accumulators to gather data for next interval
                        statAccums.getTotalCounter().setValue(new LongTriplet());
                        statAccums.getPortCounter().setValue(new HashMap<>());
                        statAccums.getIpCounter().setValue(new HashMap<>());
                        statAccums.getIpAggregationCounter().setValue(new HashMap<>());
                        
                        if (statisticsFormat.equals("CSV")) {
                            if (firstStatistics) {
                                // print header on top of CSV output
                                StringBuilder portHeader = new StringBuilder();
                                for (int i = 1; i <= 10; i++) {
                                    portHeader.append(String.format("port %s;port %s flows;port %s packets;port %s bytes;", i, i, i, i));
                                }
                                StringBuilder ipHeader = new StringBuilder();
                                for (int i = 1; i <= 10; i++) {
                                    ipHeader.append(String.format("IP %s;IP %s flows;IP %s packets;IP %s bytes;", i, i, i, i));
                                }
                                StringBuilder ipAggrHeader = new StringBuilder();
                                for (int i = 1; i <= 100; i++) {
                                    ipAggrHeader.append(String.format("IP mask %s;IP mask %s flows;IP mask %s packets;IP mask %s bytes;", i, i, i, i));
                                }
                                
                                String csvStatsHeader = String.format("%s;;%s;;%s;%s;;%s;%s;%s",
                                        "Time", "Flows;Packets;Bytes",
                                        "http flows;http packets;http bytes",
                                        "https flows;https packets;https bytes",
                                        portHeader.toString(),
                                        ipHeader.toString(),
                                        ipAggrHeader.toString());
                                printTestResult(statisticsTopic, null, 
                                        Arrays.asList(csvStatsHeader));
                                firstStatistics = false;
                            }
                            
                            // CSV output result line
                            printTestResult(statisticsTopic, null,
                                    // time, 3 total values, 3x http + 3x https, 3x 10 ports, 3x 10 IPs, aggregated IPs
                                    Arrays.asList(String.format("%s;;%s;;%s;%s;;%s;%s;%s",
                                            LocalDateTime.now().format(formatter),
                                            totalValue.toCsvString(),
                                            portCounter.getOrDefault("80", new LongTriplet()).toCsvString(),
                                            portCounter.getOrDefault("443", new LongTriplet()).toCsvString(),
                                            Utils.mapWithTripletToCsv(Utils.putFirstEntries(10, portSortedMap), 10),
                                            Utils.mapWithTripletToCsv(Utils.putFirstEntries(10, ipSortedMap), 10),
                                            Utils.mapWithTripletToCsv(ipAggregationSortedMap, 100))));
                        } else {
                            // no meaningful format specified, print statistics using Java toString methods
                            printTestResult(statisticsTopic, null, 
                                    Arrays.asList(String.format("%s;%s;%s", totalValue.getA(), totalValue.getB(), totalValue.getC())));

                            printTestResult(statisticsTopic, null, 
                                    Arrays.asList("http: " + portCounter.getOrDefault("80", new LongTriplet()).toString() +
                                            ";https: " + portCounter.getOrDefault("443", new LongTriplet()).toString()));

                            printTestResult(statisticsTopic, null, 
                                    Arrays.asList(Utils.putFirstEntries(10, portSortedMap).toString()));

                            printTestResult(statisticsTopic, null, 
                                    Arrays.asList(Utils.putFirstEntries(10, ipSortedMap).toString()));

                            printTestResult(statisticsTopic, null, 
                                    Arrays.asList(ipAggregationSortedMap.toString()));
                        }
                    }
                    
                    step++;
                }
                try {
                    Thread.sleep(500);
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
                .set("spark.default.parallelism", sparkProps.getProperty("spark.default.parallelism"))
                .set("spark.ui.port", sparkProps.getProperty("spark.ui.port"));
    }
}