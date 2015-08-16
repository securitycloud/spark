package cz.muni.fi.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.muni.fi.kafka.OutputProducer;
import cz.muni.fi.spark.accumulators.MapAccumulator;
import cz.muni.fi.spark.tests.*;
import cz.muni.fi.util.PropertiesParser;
import kafka.serializer.StringDecoder;
import kafka.utils.ZkUtils;
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
 * Spark streaming test from/to Kafka.
 */
public class App {

    static final long SPARK_STREAMING_BATCH_INTERVAL = 1000;
    static final int NUMBER_OF_STREAMS = 2; // should match the number of partitions

    private static final OutputProducer prod = new OutputProducer();

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


        String testClass = "FilterIPTest";
        Accumulator<Integer> processedRecordsCounter = jssc.sparkContext().accumulator(0);
        // STREAMING
        // Each streamed input batch forms an RDD
        switch (testClass) {
            case "ReadWriteTest": {
                messages.foreachRDD(new ReadWriteTest(processedRecordsCounter));
                break;
            }
            case "FilterIPTest": {
                messages.foreachRDD(new FilterIPTest(processedRecordsCounter));
                break;
            }
            case "CountTest": {
                Accumulator<Integer> filteredIpCount = jssc.sparkContext().accumulator(0);
                messages.foreachRDD(new CountTest(processedRecordsCounter, filteredIpCount));
                /**
                 * Thread that checks in interval whether we have reached the end of our test data.
                 * If the end was reached, then it sends a message to Kafka producer.
                 */
                Thread countTestFinishObserver = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        while (true) {
                            Integer filtered = filteredIpCount.value();
                            if (filtered == 33632090) { // filtered events out of total 36846558
                                prod.send(new Tuple2<>(null, "IP: amount of packets: " + filtered));
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
        }

        try {
            Thread.sleep(10000); // wait for spark to start processing
        } catch (InterruptedException e) {
            System.out.println(e.getLocalizedMessage());
        }

        /**
         * Thread for performance monitoring.
         */
        Thread performanceMeasuringThread = new Thread(new Runnable() {
            @Override
            public void run() {
                Long min = 0L;
                Long max = 0L;

                LocalDateTime startDateTime = LocalDateTime.now();
                while (true) {
                    Integer processedRecords = processedRecordsCounter.value();
                    if (processedRecords != 0) {
                        Long processingTimeInMillis = ChronoUnit.MILLIS.between(startDateTime, LocalDateTime.now());
                        if (processingTimeInMillis >= 1000) {
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
                            System.out.println("average speed: " + averageSpeed + " records/s");
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
     * Takes configuration from pom.xml -> spark.properties and returns it as {@link SparkConf}.
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