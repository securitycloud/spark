package cz.muni.fi.spark;

import cz.muni.fi.util.PropertiesParser;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


/**
 * Spark streaming test from Kafka.
 */
public class App {

    static final long BATCH_INTERVAL = 1000;

    public static void main(String[] args) {
        final SparkConf sparkConf = ConfigurationProducer.getSparkConf();
        final Properties kafkaProps = new PropertiesParser().getKafkaProperties();
        // Spark Streaming init
        final JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.milliseconds(BATCH_INTERVAL));
        Map<String, Integer> topicMap = new HashMap<>();
        topicMap.put("test", 1); // topic, numThreads
        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, kafkaProps.getProperty("zookeeper.url"), "1", topicMap);
        //JSONFlattener jf = new JSONFlattener(new ObjectMapper());
        messages.foreachRDD(rdd -> { // Each streamed input batch forms an RDD
            //rdd.persist(StorageLevel.MEMORY_ONLY());
            rdd.foreach(line -> {
                //Map<String, Object> msg = jf.jsonToFlatMap(line);
                System.out.println(line);
            });

            System.out.println(rdd.count());
            return null;
        });
        jssc.start();
        jssc.awaitTermination();
    }
}