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

import java.util.*;


/**
 * Spark streaming test from/to Kafka.
 */
public class App {

    static final long SPARK_STREAMING_BATCH_INTERVAL = 1000;
    static final int NUMBER_OF_STREAMS = 5; // should match the number of partitions

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


        String testClass = "ReadWriteTest";
        // STREAMING
        // Each streamed input batch forms an RDD
        switch(testClass) {
            case "ReadWriteTest": {
                messages.foreachRDD(new ReadWriteTest());
                break;
            }
            case "FilterIPTest": {
                messages.foreachRDD(new FilterIPTest());
                break;
            }
            case "CountTest": {
                Accumulator<Integer> filteredIpCount = jssc.sparkContext().accumulator(0);
                messages.foreachRDD(new CountTest(filteredIpCount));
                /**
                 * Thread that checks in interval whether we have reached the end of our test data.
                 * If the end was reached, then it sends a message to Kafka producer.
                 */
                Thread countTestObserver = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        while (true) {
                            Integer count = filteredIpCount.value();
                            if (count == 33632090) { // filtered events out of total 36846558
                                prod.send(new Tuple2<>(null, "IP: amount of packets: " + count));
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
                countTestObserver.start();
                break;
            }
        }

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
                .set("spark.streaming.receiver.maxRate", "0");
    }
}