package cz.muni.fi.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.muni.fi.json.JSONFlattener;
import cz.muni.fi.kafka.OutputProducer;
import cz.muni.fi.util.PropertiesParser;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


/**
 * Spark streaming test from/to Kafka.
 */
public class App {

    static final long BATCH_INTERVAL = 1000;

    public static void main(String[] args) {
        final SparkConf sparkConf = getSparkConf();
        final Properties kafkaProps = PropertiesParser.getKafkaProperties();
        final JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.milliseconds(BATCH_INTERVAL));

        Map<String, Integer> topicMap = new HashMap<>();
        topicMap.put(kafkaProps.getProperty("consumer.topic"), 1); // topic, numThreads

        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, kafkaProps.getProperty("zookeeper.url"), "1", topicMap);

        final JSONFlattener jsonFlattener = new JSONFlattener(new ObjectMapper());
        // Each streamed input batch forms an RDD
        messages.foreachRDD(rdd -> {
            rdd.foreachPartition(it -> {
                final OutputProducer prod = new OutputProducer();
                while (it.hasNext()) {
                    Tuple2<String, String> msg = it.next();
                    Map<String, Object> json = jsonFlattener.jsonToFlatMap(msg._2());
                    if (json.get("src_port").equals(49646)) { // filter by src_port 49646
                        prod.sendJson(new Tuple2<>(null, json));
                    }
                }
            });
            return null;
        });

        jssc.start();
        jssc.awaitTermination();
    }

    public static SparkConf getSparkConf() {
        final Properties sparkProps = PropertiesParser.getSparkProperties();
        return new SparkConf()
                .setSparkHome(sparkProps.getProperty("spark.home"))
                .setAppName(sparkProps.getProperty("spark.app.name"))
                .set("spark.ui.port", sparkProps.getProperty("spark.ui.port"))
                .setMaster(sparkProps.getProperty("spark.master.url"))
                .set("spark.executor.memory", sparkProps.getProperty("spark.executor.memory"))
                .set("spark.serializer", sparkProps.getProperty("spark.serializer"));
    }
}