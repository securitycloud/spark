package cz.muni.fi.spark.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.muni.fi.commons.Flow;
import org.apache.spark.Accumulator;
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
 * At the end, merges the map with shared accumulator map of all occurrences.
 * Uses object mapper and flow POJO to convert and store JSON messages.
 */
public class AggregationTest implements Function<JavaPairRDD<String, String>, Void> {
    private static final ObjectMapper mapper = new ObjectMapper();

    private Accumulator<Integer> processedRecordsCounter;
    private Accumulator<Map<String, Integer>> ipOccurrences;

    /**
     * Initializes Aggregation test class with passed Accumulators.
     *
     * @param processedRecordsCounter Accumulator with total of processed records
     * @param ipOccurrences Map<String, Integer> Accumulator with all the found IP addresses and their occurences
     */
    public AggregationTest(Accumulator<Integer> processedRecordsCounter, Accumulator<Map<String, Integer>> ipOccurrences) {
        this.processedRecordsCounter = processedRecordsCounter;
        this.ipOccurrences = ipOccurrences;
    }

    @Override
    public Void call(JavaPairRDD<String, String> rdd) throws IOException {
        rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
            @Override
            public void call(Iterator<Tuple2<String, String>> it) throws IOException {
                Map<String, Integer> tempIpOccurences = new HashMap<>();
                Integer tempCount = 0;
                while (it.hasNext()) {
                    Tuple2<String, String> msg = it.next();
                    Flow flow = mapper.readValue(msg._2(), Flow.class);
                    if (!tempIpOccurences.containsKey(flow.getDst_ip_addr())) { // put in new key with value 1
                        tempIpOccurences.put(flow.getDst_ip_addr(), 1);
                    } else { // increment existing key value
                        tempIpOccurences.put(flow.getDst_ip_addr(), tempIpOccurences.get(flow.getDst_ip_addr()) + 1);
                    }
                    tempCount++;
                }
                ipOccurrences.add(tempIpOccurences);
                processedRecordsCounter.add(tempCount);
            }
        });
        return null;
    }
}