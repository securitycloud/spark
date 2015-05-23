package cz.muni.fi.spark;

import cz.muni.fi.kafka.Producer;
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
        final SparkConf sparkConf = SparkConfProducer.getSparkConf();
        final Properties kafkaProps = PropertiesParser.getKafkaProperties();
        final JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.milliseconds(BATCH_INTERVAL));

        Map<String, Integer> topicMap = new HashMap<>();
        topicMap.put(kafkaProps.getProperty("consumer.topic"), 1); // topic, numThreads

        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, kafkaProps.getProperty("zookeeper.url"), "1", topicMap);

        messages.foreachRDD(rdd -> { // Each streamed input batch forms an RDD
            rdd.foreachPartition(it -> {
                final Producer kafkaProd = new Producer();
                while(it.hasNext()) {
                    Tuple2<String, String> msg = it.next();
                    System.out.println("sending to kafka: "+msg._2());
                    kafkaProd.send(it.next());
                }
            });
            //System.out.println(rdd.count());
            return null;
        });

        jssc.start();
        jssc.awaitTermination();
    }
}