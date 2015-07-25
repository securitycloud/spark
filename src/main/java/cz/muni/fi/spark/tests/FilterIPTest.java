package cz.muni.fi.spark.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.muni.fi.commons.Flow;
import cz.muni.fi.commons.JSONFlattener;
import cz.muni.fi.kafka.OutputProducer;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Access every message read, convert to json and check if IP matches, if yes, then send it to Kafka Output.
 */
public class FilterIPTest implements Function<JavaPairRDD<String, String>, Void> {
    private static final String FILTERED_IP = "62.148.241.49";

    private static final OutputProducer prod = new OutputProducer();
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final JSONFlattener jsonFlattener = new JSONFlattener(new ObjectMapper());

    // USING OBJECT MAPPER AND FLOW POJO
    @Override
    public Void call(JavaPairRDD<String, String> rdd) throws IOException {
        rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
            @Override
            public void call(Iterator<Tuple2<String, String>> it) throws IOException {
                while (it.hasNext()) { // for each event in partition
                    Tuple2<String, String> msg = it.next();
                    Flow flow = mapper.readValue(msg._2(), Flow.class);
                    if (flow.getDst_ip_addr().equals(FILTERED_IP)) {
                        prod.send(new Tuple2<>(null, flow.toString()));
                    }
                }
            }
        });
        return null;
    }

    // USING JSON FLATTENER
    /*@Override
    public Void call(JavaPairRDD<String, String> rdd) throws IOException {
        rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
            @Override
            public void call(Iterator<Tuple2<String, String>> it) throws IOException {
                while (it.hasNext()) { // for each event in partition
                    Tuple2<String, String> msg = it.next();
                    Map<String, Object> json = jsonFlattener.jsonToFlatMap(msg._2());
                    if (json.get("dst_ip_addr").equals("62.148.241.49")) {
                        prod.sendMap(new Tuple2<>(null, json));
                    }
                }
            }
        });
        return null;
    }*/
}