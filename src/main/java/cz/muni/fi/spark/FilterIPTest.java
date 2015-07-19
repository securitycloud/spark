package cz.muni.fi.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.muni.fi.json.JSONFlattener;
import cz.muni.fi.kafka.OutputProducer;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Provides nested classes (to be passed instead of anonymous classes to Spark) for testing of R/W only with IP filter.
 */
public class FilterIPTest {

    /**
     * Access every message read, convert to json and check if IP matches.
     */
    public static class ReadOnly implements Function<JavaPairRDD<String, String>, Void> {
        public static final JSONFlattener jsonFlattener = new JSONFlattener(new ObjectMapper());

        @Override
        public Void call(JavaPairRDD<String, String> rdd) throws IOException {
            rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
                @Override
                public void call(Iterator<Tuple2<String, String>> it) throws IOException {
                    while (it.hasNext()) { // for each event in partition
                        Tuple2<String, String> msg = it.next();
                        Map<String, Object> json = jsonFlattener.jsonToFlatMap(msg._2());
                        if (json.get("dst_ip_addr").equals("62.148.241.49")) {
                        }
                    }
                }
            });
            return null;
        }
    }

    /**
     * Access every message read, convert to json and check if IP matches, if yes, then send it to Kafka Output.
     */
    public static class ReadWrite implements Function<JavaPairRDD<String, String>, Void> {
        public static final OutputProducer prod = new OutputProducer();
        public static final JSONFlattener jsonFlattener = new JSONFlattener(new ObjectMapper());

        @Override
        public Void call(JavaPairRDD<String, String> rdd) throws IOException {
            rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
                @Override
                public void call(Iterator<Tuple2<String, String>> it) throws IOException {
                    while (it.hasNext()) { // for each event in partition
                        Tuple2<String, String> msg = it.next();
                        Map<String, Object> json = jsonFlattener.jsonToFlatMap(msg._2());
                        if (json.get("dst_ip_addr").equals("62.148.241.49")) {
                            prod.sendJson(new Tuple2<>(null, json));
                        }
                    }
                }
            });
            return null;
        }
    }
}
