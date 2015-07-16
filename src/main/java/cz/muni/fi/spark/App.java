package cz.muni.fi.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.muni.fi.json.JSONFlattener;
import cz.muni.fi.kafka.OutputProducer;
import cz.muni.fi.util.PropertiesParser;
import java.io.FileNotFoundException;
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
import java.io.PrintWriter;
import java.util.*;
import org.apache.spark.Accumulator;
import org.apache.spark.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function0;
import scala.runtime.BoxedUnit;


/**
 * Spark streaming test from/to Kafka.
 * Args: Number of streams, IP to filter
 */
public class App {
    
    private static final Logger log = LoggerFactory.getLogger(App.class);

    static final long BATCH_INTERVAL = 1000;
    static final int NUMBER_OF_STREAMS = 1; //193048.13, 133297.78

    public static final JSONFlattener jsonFlattener = new JSONFlattener(new ObjectMapper());
    public static final OutputProducer prod = new OutputProducer();
    
    private static volatile int flowCount = 0;
    private static final Object counterLock = new Object();
    private static long timeStart = new Date().getTime();
    
    private static String lastLog;

    public static void main(String[] args) {
        final SparkConf sparkConf = getSparkConf();
        final Properties kafkaProps = PropertiesParser.getKafkaProperties();
        final JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.milliseconds(BATCH_INTERVAL));
        
        int tmpStreamsCount = NUMBER_OF_STREAMS;
        if (args.length > 0) {
            try {
                tmpStreamsCount = Integer.parseInt(args[0]);
            } catch (NumberFormatException numberFormatException) {
            }
        }
        final int streamsCount = tmpStreamsCount;
        
        final String filterIP = (args.length > 1 ? args[1] : null);
        
        Accumulator<Integer> accum = jssc.sparkContext().accumulator(0);
        logMain("-- starting Spark app: " + streamsCount + " streams, " +
                (filterIP == null ? "no IP filter" : "filter IP " + filterIP));

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

        List<JavaPairDStream<String, String>> kafkaStreams = new ArrayList<>(streamsCount);
        for (int i = 0; i < streamsCount; i++) {
            // standard basic stream creation
            //kafkaStreams.add(KafkaUtils.createStream(jssc, kafkaProps.getProperty("zookeeper.url"), "1", topicMap));

            // advanced stream creation with kafka properties as parameter
            kafkaStreams.add(KafkaUtils.createStream(jssc, String.class, String.class, StringDecoder.class,
                    StringDecoder.class, kafkaPropsMap, topicMap, StorageLevel.MEMORY_AND_DISK_SER_2()));

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
                            
                            //prod.send(msg); // send message string to output
                            synchronized (counterLock) {
                                /*flowCount++;
                                if (flowCount % 100000 == 0) {
                                    long secFromStart = (new Date().getTime() - timeStart) / 1000;
                                    logMain((float)flowCount / secFromStart + "K f/s - total: " + 
                                            (float)flowCount / 1000000 + "M flows at " + secFromStart + " sec");
                                }*/
                                accum.add(1);
                                /*if (accum.localValue() % 100000 == 0) {
                                    long secFromStart = (new Date().getTime() - timeStart) / 1000;
                                    logMain((float)accum.localValue() / secFromStart + "K f/s - total: " + 
                                            (float)accum.localValue() / 1000000 + "M flows at " + secFromStart + " sec");
                                }*/
                            }
                            
                            Map<String, Object> json = jsonFlattener.jsonToFlatMap(msg._2());
                            if (filterIP != null && json.get("dst_ip_addr").equals(filterIP)) { // filter
                                prod.sendJson(new Tuple2<>(null, json)); // send to kafka output
                            }
                            
                            /**
                             * How to test
                             * 1) Read only:
                             * Tuple2<String, String> msg = it.next();
                             * 2) R/W:
                             * Tuple2<String, String> msg = it.next();
                             * prod.send(msg);
                             * 3) Read only with filter:
                             * Tuple2<String, String> msg = it.next();
                             * Map<String, Object> json = jsonFlattener.jsonToFlatMap(msg._2());
                             * if (json.get("dst_ip_addr").equals("62.148.241.49")) {
                             *
                             * }
                             * 4) R/W with filter:
                             * Tuple2<String, String> msg = it.next();
                             * prod.send(msg); // send message string to output
                             * Map<String, Object> json = jsonFlattener.jsonToFlatMap(msg._2());
                             * if (json.get("dst_ip_addr").equals("62.148.241.49")) {
                             *      prod.sendJson(new Tuple2<>(null, json));
                             * }
                             */
                        }
                    }
                });
                return null;
            }
        });
        
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    PrintWriter pw = new PrintWriter(String.format("/root/mjelinek/sparkresults/%s_%sST_%s.txt", timeStart, streamsCount, filterIP != null ? "IP" : "all"));
                    pw.append("-- Spark app: " + streamsCount + " streams, " +
                        (filterIP == null ? "no IP filter" : "filter IP " + filterIP) + "\n");
                    long secFromStart = (new Date().getTime() - timeStart) / 1000;
                    pw.append((float)accum.value() / secFromStart + "K f/s - total: " + 
                            (float)accum.value() / 1000000 + "M flows at " + secFromStart + " sec");
                    pw.close();
                } catch (FileNotFoundException ex) {
                    
                }
                jssc.stop(true, false);
            }
        }));

        jssc.start();
        jssc.awaitTermination();
    }
    
    public static void logMain(String message) {
        System.out.println(message);
        log.info(message);
        lastLog = message;
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