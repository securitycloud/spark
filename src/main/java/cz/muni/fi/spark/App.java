package cz.muni.fi.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.muni.fi.util.JSONFlattener;
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
 * Created by filip on 12.4.15.
 */
public class App {

    static final long BATCH_INTERVAL = 1000;

    public static void main(String[] args) {
        final SparkConf sparkConf = getSparkConf();
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

    public static SparkConf getSparkConf() {
        final Properties sparkProps = new PropertiesParser().getSparkProperties();
        return new SparkConf()
                .setSparkHome(sparkProps.getProperty("spark.home"))
                .setAppName(sparkProps.getProperty("spark.app.name"))
                .set("spark.ui.port", sparkProps.getProperty("spark.ui.port"))
                .setMaster(sparkProps.getProperty("spark.master.url"))
                .set("spark.executor.memory", sparkProps.getProperty("spark.executor.memory"))
                .set("spark.serializer", sparkProps.getProperty("spark.serializer"));
    }
}