package cz.muni.fi.spark;

import cz.muni.fi.commons.MapValueComparator;
import cz.muni.fi.commons.Triplet;
import cz.muni.fi.kafka.OutputProducer;
import cz.muni.fi.spark.accumulators.MapAccumulator;
import cz.muni.fi.spark.tests.*;
import cz.muni.fi.util.PropertiesParser;
import kafka.serializer.StringDecoder;
import kafka.utils.ZkUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;


/**
 * Class to be submitted to spark-submit.
 */
public class App {

    static final long SPARK_STREAMING_BATCH_INTERVAL = 1000;
    static final int NUMBER_OF_STREAMS = 3; // number of kafka streams, should be less or equal to the number of kafka partitions
    static final String FILTER_TEST_IP = "62.148.241.49";
    static final long TEST_DATA_RECORDS_SIZE = 36846558; // total amount of events in our data set

    private static final OutputProducer prod = new OutputProducer();

    /**
     * @param args
     */
    public static void main(String[] args) {
        final SparkConf sparkConf = getSparkConf();
        final Properties kafkaProps = PropertiesParser.getKafkaProperties();
        final JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.milliseconds(SPARK_STREAMING_BATCH_INTERVAL));

        Map<String, Integer> topicMap = new HashMap<>();
        topicMap.put(kafkaProps.getProperty("consumer.topic"), 1); // topic, numThreads

        //Set<String> topicSet = new HashSet<>();
        //topicSet.add(kafkaProps.getProperty("consumer.topic")); // topic

        Map<String, String> kafkaPropsMap = new HashMap<>();
        for (String key : kafkaProps.stringPropertyNames()) {
            kafkaPropsMap.put(key, kafkaProps.getProperty(key));
        }

        // reset zookeeper data for group so all messages from topic beginning can be read
        ZkUtils.maybeDeletePath(kafkaProps.getProperty("zookeeper.url"), "/consumers/" + kafkaProps.getProperty("group.id"));

        List<JavaPairDStream<String, String>> kafkaStreams = new ArrayList<>(NUMBER_OF_STREAMS);
        for (int i = 0; i < NUMBER_OF_STREAMS; i++) {
            // standard basic stream creation
            // kafkaStreams.add(KafkaUtils.createStream(jssc, kafkaProps.getProperty("zookeeper.url"), "1", topicMap));

            // advanced stream creation with kafka properties as parameter
            kafkaStreams.add(KafkaUtils.createStream(jssc, String.class, String.class, StringDecoder.class,
                    StringDecoder.class, kafkaPropsMap, topicMap, StorageLevel.MEMORY_AND_DISK_SER()));

            // direct stream - new approach test
            //kafkaStreams.add(KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaPropsMap, topicSet));
        }
        JavaPairDStream<String, String> messages = jssc.union(kafkaStreams.get(0), kafkaStreams.subList(1, kafkaStreams.size()));

        final String testClass = "SynScanTest"; // to be redone into command line argument, name of test class to be run
        Accumulator<Integer> processedRecordsCounter = jssc.sparkContext().accumulator(0);
        // STREAMING, Each streamed input batch forms an RDD
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
                Accumulator<Integer> filteredIpCount = jssc.sparkContext().accumulator(0);
                messages.foreachRDD(new CountTest(processedRecordsCounter, filteredIpCount));
                // Checks in interval whether we have reached the end of our test data and sends message to Kafka on end.
                Thread countTestFinishObserver = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        while (true) {
                            Integer processedRecords = processedRecordsCounter.value();
                            if (processedRecords == TEST_DATA_RECORDS_SIZE) { // filtered events out of total 36846558,
                                prod.send(new Tuple2<>(null, "IP: " + FILTER_TEST_IP + ", amount of packets: " + filteredIpCount.value())); // kafka consumer dostane jednu zpravu obsahující IP a sumu paketu
                                break;
                            }
                            try {
                                Thread.sleep(500);
                            } catch (InterruptedException e) {
                                System.out.println(e.getLocalizedMessage());
                            }
                        }
                    }
                });
                countTestFinishObserver.start();
                break;
            }
            case "AggregationTest": {
                Accumulator<Map<String, Integer>> ipPackets = jssc.sparkContext().accumulator(new HashMap<>(), new MapAccumulator());
                messages.foreachRDD(new AggregationTest(processedRecordsCounter, ipPackets));
                // Checks in interval whether we have reached the end of our test data and sends message to Kafka on end.
                Thread aggregationTestFinishObserver = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        while (true) {
                            Integer processedRecords = processedRecordsCounter.value();
                            if (processedRecords == TEST_DATA_RECORDS_SIZE) { // kafka-consumer dostane po zpracování datové sady mapu dvojic [dst IP, počet packetů]
                                prod.send(new Tuple2<>(null, ipPackets.value().toString()));
                                break;
                            }
                            try {
                                Thread.sleep(500);
                            } catch (InterruptedException e) {
                                System.out.println(e.getLocalizedMessage());
                            }
                        }
                    }
                });
                aggregationTestFinishObserver.start();
                break;
            }
            case "TopNTest": {
                Accumulator<Map<String, Integer>> ipPackets = jssc.sparkContext().accumulator(new HashMap<>(), new MapAccumulator());
                messages.foreachRDD(new AggregationTest(processedRecordsCounter, ipPackets));
                // Checks in interval whether we have reached the end of our test data and sends message to Kafka on end.
                Thread topNTestFinishObserver = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        while (true) {
                            Integer processedRecords = processedRecordsCounter.value();
                            if (processedRecords == TEST_DATA_RECORDS_SIZE) {
                                Map<String, Integer> total = ipPackets.value();
                                MapValueComparator valueComparator = new MapValueComparator(total);
                                TreeMap<String, Integer> sortedTotal = new TreeMap<>(valueComparator);
                                sortedTotal.putAll(total);
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
                                prod.send(new Tuple2<>(null, topElements.toString()));
                            }
                            try {
                                Thread.sleep(500);
                            } catch (InterruptedException e) {
                                System.out.println(e.getLocalizedMessage());
                            }
                        }
                    }
                });
                topNTestFinishObserver.start();
                break;
            }
            case "SynScanTest": {
                Accumulator<Map<String, Integer>> ipOccurrences = jssc.sparkContext().accumulator(new HashMap<>(), new MapAccumulator());
                messages.foreachRDD(new SynScanTest(processedRecordsCounter, ipOccurrences));
                // Checks in interval whether we have reached the end of our test data and sends message to Kafka on end.
                Thread synScanTestFinishObserver = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        while (true) {
                            Integer processedRecords = processedRecordsCounter.value();
                            if (processedRecords == TEST_DATA_RECORDS_SIZE) {
                                Map<String, Integer> total = ipOccurrences.value();
                                Map<String, Integer> filtered = SynScanTest.filterMap(total);
                                System.out.println("all: " + total.size());
                                System.out.println("filtered: " + filtered.size());
                                prod.send(new Tuple2<>(null, filtered.toString()));
                                break;
                            }
                            try {
                                Thread.sleep(500);
                            } catch (InterruptedException e) {
                                System.out.println(e.getLocalizedMessage());
                            }
                        }
                    }
                });
                synScanTestFinishObserver.start();
                break;
            }
            default: {
                throw new IllegalArgumentException("test class name does not exist: " + testClass);
            }
        }

        /**
         * Thread for performance monitoring. Updates min and max with average that is taken every 5 seconds.
         */
        Thread performanceMeasuringThread = new Thread(new Runnable() {
            @Override
            public void run() {
                Long min = 0L;
                Long max = 0L;
                LocalDateTime startDateTime = LocalDateTime.now();
                while (true) {
                    Integer processedRecords = processedRecordsCounter.value();
                    if (processedRecords > 0) {
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
                            System.out.println("avg speed: " + averageSpeed + " records/s");
                            System.out.println("min speed: " + min + " records/s");
                            System.out.println("max speed: " + max + " records/s");
                        }
                    }
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        System.out.println(e.getLocalizedMessage());
                    }
                }
            }
        });
        performanceMeasuringThread.start();

        jssc.start();
        jssc.awaitTermination();
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