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
 * Computes a key value map with dst IP addresses and the amount of their packets in the stream.
 * At the end, merges the map with shared accumulator map of all occurrences.
 * Uses object mapper and flow POJO to convert and store JSON messages.
 */
public class AggregationTest implements VoidFunction<JavaPairRDD<String, String>> {
    private static final ObjectMapper mapper = new ObjectMapper();

    private Accumulator<Integer> processedRecordsCounter;
    private Accumulator<Map<String, Integer>> ipPackets;

    /**
     * Initializes Aggregation test class with passed Accumulators.
     *
     * @param processedRecordsCounter Accumulator with total of processed records
     * @param ipPackets               Map<String, Integer> Accumulator with all the found IP addresses and their occurrences
     */
    public AggregationTest(Accumulator<Integer> processedRecordsCounter, Accumulator<Map<String, Integer>> ipPackets) {
        this.processedRecordsCounter = processedRecordsCounter;
        this.ipPackets = ipPackets;
    }

    /**
     * Applies a function call to each partition of this RDD.
     * Function call parses each message on the partition into Flow and counts packets of destination ip addresses
     * into a temporary map that is merged at the end on node with driver to shared map of total packet counts.
     * At the end updates a count of total processed messages with records processed on the particular node where this
     * method is run.
     *
     * @param rdd batch to be processed
     * @throws IOException on ObjectMapper error
     */
    @Override
    public void call(JavaPairRDD<String, String> rdd) throws IOException {
        rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
            @Override
            public void call(Iterator<Tuple2<String, String>> it) throws IOException {
                Map<String, Integer> tempIpPackets = new HashMap<>();
                Integer tempCount = 0;
                while (it.hasNext()) {
                    Tuple2<String, String> msg = it.next();
                    Flow flow = mapper.readValue(msg._2(), Flow.class);
                    if (!tempIpPackets.containsKey(flow.getDst_ip_addr())) { // put in new key
                        tempIpPackets.put(flow.getDst_ip_addr(), flow.getPackets());
                    } else { // increment existing value
                        tempIpPackets.put(flow.getDst_ip_addr(), tempIpPackets.get(flow.getDst_ip_addr()) + flow.getPackets());
                    }
                    tempCount++;
                }
                ipPackets.add(tempIpPackets);
                processedRecordsCounter.add(tempCount);
            }
        });
    }
}