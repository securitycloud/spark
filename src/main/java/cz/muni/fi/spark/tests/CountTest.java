package cz.muni.fi.spark.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.muni.fi.commons.Flow;
import cz.muni.fi.kafka.OutputProducer;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.Iterator;

/**
 * Access every message read, convert to json and check if IP matches, if yes, increase counter.
 * Send a total count message to kafka producer at the end.
 */
public class CountTest implements Function<JavaPairRDD<String, String>, Void> {
    private static final String FILTERED_IP = "62.148.241.49";

    private static final OutputProducer prod = new OutputProducer();
    private static final ObjectMapper mapper = new ObjectMapper();

    //private static final int counterWindow = 1000000;

    private Accumulator<Integer> totalCount;

    public CountTest(Accumulator<Integer> accum) {
        this.totalCount = accum;
    }

    @Override
    public Void call(JavaPairRDD<String, String> rdd) throws IOException {
        /*JavaPairRDD<String, String> filteredRDD = rdd.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> tuple) throws Exception {
                Flow flow = mapper.readValue(tuple._2(), Flow.class);
                if (flow.getDst_ip_addr().equals(FILTERED_IP)) {
                    return true;
                }
                return false;
            }
        });*/
        rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String,String>>>() {
            @Override
            public void call(Iterator<Tuple2<String, String>> it) throws IOException {
                while (it.hasNext()) { // for each event in partition
                    Tuple2<String, String> msg = it.next();
                    Flow flow = mapper.readValue(msg._2(), Flow.class);
                    if (flow.getDst_ip_addr().equals(FILTERED_IP)) {
                        totalCount.add(1);
                    }
                }
            }
        });
        prod.send(new Tuple2<>(null, "IP: " + FILTERED_IP + ", amount of packets: " + totalCount.value()));
        return null;
    }
}