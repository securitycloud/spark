package cz.muni.fi.spark.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.muni.fi.commons.Flow;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Computes a key value map with src IP addresses and the amount of their packets in the stream.
 * At the end, merges the map with shared accumulator map of all occurrences.
 * Uses object mapper and flow POJO to convert and store JSON messages.
 */
public class SynScanTest implements Function<JavaPairRDD<String, String>, Void> {
    private static final ObjectMapper mapper = new ObjectMapper();

    private Accumulator<Integer> processedRecordsCounter;
    private Accumulator<Map<String, Integer>> ipOccurrences;

    /**
     * Initializes SynScan test class with passed Accumulators.
     *
     * @param processedRecordsCounter Accumulator with total of processed records
     * @param ipOccurrences           Map<String, Integer> Accumulator with all the found IP addresses and their occurrences
     */
    public SynScanTest(Accumulator<Integer> processedRecordsCounter, Accumulator<Map<String, Integer>> ipOccurrences) {
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
                    if (flow.getFlags().equals("....S.")) {
                        if (!tempIpOccurences.containsKey(flow.getSrc_ip_addr())) { // put in new key
                            tempIpOccurences.put(flow.getSrc_ip_addr(), 1);
                        } else { // increment existing value
                            tempIpOccurences.put(flow.getSrc_ip_addr(), tempIpOccurences.get(flow.getSrc_ip_addr()) + 1);
                        }
                    }
                    tempCount++;
                }
                ipOccurrences.add(filterMap(tempIpOccurences, 1));
                processedRecordsCounter.add(tempCount);
            }
        });
        return null;
    }

    /**
     * Takes a map of ip addresses and their occurrences and removes elements with less than 11 occurrences
     *
     * @param map map to be filtered
     * @param value value that the flow count is discarded if its lees than or equal to
     * @return filtered map
     */
    public static Map<String, Integer> filterMap(Map<String, Integer> map, Integer value) {
        Iterator it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Integer> pair = (Map.Entry<String, Integer>) it.next();
            if (pair.getValue() <= value) {
                it.remove(); // avoids a ConcurrentModificationException
            }
        }
        return map;
    }
}