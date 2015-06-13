package cz.muni.fi.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.muni.fi.json.JSONFlattener;
import cz.muni.fi.kafka.OutputProducer;
import cz.muni.fi.util.PropertiesParser;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;
import kafka.utils.ZkUtils;


/**
 * Spark streaming test from/to Kafka.
 */
public class App {

    static final long BATCH_INTERVAL = 1000;
    
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

        //Map<String, String> kafkaPropsMap = new HashMap<String, String>();
        //for (String key : kafkaProps.stringPropertyNames()) {
        //    kafkaPropsMap.put(key, kafkaProps.getProperty(key));
        //}
        
        // reset zookeeper data for group so all messages from topic beginning can be read
        //ZkUtils.maybeDeletePath(kafkaProps.getProperty("zookeeper.url"), 
        //        "/consumers/" + kafkaProps.getProperty("group.id"));

        int numStreams = 3;
        List<JavaPairDStream<String, String>> kafkaStreams = new ArrayList<>(numStreams);
        for (int i = 0; i < numStreams; i++) {
            // standard basic stream creation
            kafkaStreams.add(KafkaUtils.createStream(jssc, kafkaProps.getProperty("zookeeper.url"), "1", topicMap));
            
            // advanced stream creation with kafka properties as parameter
            //kafkaStreams.add(KafkaUtils.createStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class,
            //        kafkaPropsMap, topicMap, StorageLevel.MEMORY_AND_DISK_SER_2()));
            
            // direct stream - new approach test
            //kafkaStreams.add(KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaPropsMap, topicSet));
        } 
        
        JavaPairDStream<String, String> messages = jssc.union(kafkaStreams.get(0), kafkaStreams.subList(1, kafkaStreams.size()));
        
        // Each streamed input batch forms an RDD
        messages.foreachRDD(new FunctionImpl());

        jssc.start();
        jssc.awaitTermination();
    }

    public static SparkConf getSparkConf() {
        final Properties sparkProps = PropertiesParser.getSparkProperties();
        return new SparkConf()
                .setSparkHome(sparkProps.getProperty("spark.home"))
                .setAppName(sparkProps.getProperty("spark.app.name"))
                .setMaster(sparkProps.getProperty("spark.master.url"))
                .set("spark.executor.memory", sparkProps.getProperty("spark.executor.memory"))
                .set("spark.serializer", sparkProps.getProperty("spark.serializer"));
    }

    private static class FunctionImpl implements Function<JavaPairRDD<String, String>, Void> {

        public FunctionImpl() {
        }

        @Override
        public Void call(JavaPairRDD<String, String> rdd) throws Exception {
            rdd.foreachPartition(new VoidFunctionImpl());
            return null;
        }

        private static class VoidFunctionImpl implements VoidFunction<Iterator<Tuple2<String, String>>> {

            public VoidFunctionImpl() {
            }

            @Override
            public void call(Iterator<Tuple2<String, String>> it) throws Exception {
                while (it.hasNext()) {
                    Tuple2<String, String> msg = it.next();
                    Map<String, Object> json = jsonFlattener.jsonToFlatMap(msg._2());
                    if (json.get("dst_port").equals(80)) { // filter by
                        prod.sendJson(new Tuple2<>(null, json));
                    }
                }
            }
        }
    }
}