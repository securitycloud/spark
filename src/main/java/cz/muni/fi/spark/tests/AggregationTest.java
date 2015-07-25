package cz.muni.fi.spark.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.muni.fi.commons.Flow;
import cz.muni.fi.kafka.OutputProducer;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Computes a key value map with IP addresses and the amount of their occurrences in the stream.
 * Sends the key value map as string to kafka producer at the end.
 */
public class AggregationTest implements Function<JavaPairRDD<String, String>, Void> {
    private static final OutputProducer prod = new OutputProducer();
    private static final ObjectMapper mapper = new ObjectMapper();
    private static Map<String, Integer> count = new HashMap<>();

    @Override
    public Void call(JavaPairRDD<String, String> rdd) throws IOException {
        rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
            @Override
            public void call(Iterator<Tuple2<String, String>> it) throws IOException {
                while (it.hasNext()) { // for each event in partition
                    Tuple2<String, String> msg = it.next();
                    Flow flow = mapper.readValue(msg._2(), Flow.class);
                    if (!count.containsKey(flow.getDst_ip_addr())) { // put in new key with value 1
                        count.put(flow.getDst_ip_addr(), 1);
                    } else { // increment existing key value
                        count.put(flow.getDst_ip_addr(), count.get(flow.getDst_ip_addr()) + 1);
                    }
                }
            }
        });
        prod.sendMap(new Tuple2<>(null, count));
        return null;
    }
}