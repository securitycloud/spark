package cz.muni.fi.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.muni.fi.json.JSONFlattener;
import cz.muni.fi.kafka.OutputProducer;
import cz.muni.fi.util.PropertiesParser;
import kafka.serializer.StringDecoder;
import kafka.utils.ZkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;


/**
 * Spark streaming test from/to Kafka.
 */
public class App {

    static final long BATCH_INTERVAL = 1000;
    static final int NUMBER_OF_STREAMS = 5;

    public static final JSONFlattener jsonFlattener = new JSONFlattener(new ObjectMapper());
    public static final OutputProducer prod = new OutputProducer();

    public static void main(String[] args) {
        final SparkConf sparkConf = getSparkConf();
        final Properties kafkaProps = PropertiesParser.getKafkaProperties();
        final JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.milliseconds(BATCH_INTERVAL));

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
            //kafkaStreams.add(KafkaUtils.createStream(jssc, kafkaProps.getProperty("zookeeper.url"), "1", topicMap));

            // advanced stream creation with kafka properties as parameter
            kafkaStreams.add(KafkaUtils.createStream(jssc, String.class, String.class, StringDecoder.class,
                    StringDecoder.class, kafkaPropsMap, topicMap, StorageLevel.MEMORY_ONLY_SER()));

            // direct stream - new approach test
            //kafkaStreams.add(KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaPropsMap, topicSet));
        }

        JavaPairDStream<String, String> messages = jssc.union(kafkaStreams.get(0), kafkaStreams.subList(1, kafkaStreams.size()));

        // STREAMING
        // Each streamed input batch forms an RDD
        messages.foreachRDD(new Function<JavaPairRDD<String, String>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, String> rdd) throws IOException {
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, String>> it) throws IOException {
                        while (it.hasNext()) { // for each event in partition
                            Tuple2<String, String> msg = it.next();
                            prod.send(msg); // send message string to output
                            /*Map<String, Object> json = jsonFlattener.jsonToFlatMap(msg._2());
                            if (json.get("dst_port").equals(80)) { // filter
                                prod.sendJson(new Tuple2<>(null, json)); // send to kafka output
                            }*/
                        }
                    }
                });
                return null;
            }
        });

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
                .set("spark.serializer", sparkProps.getProperty("spark.serializer"));
    }
}