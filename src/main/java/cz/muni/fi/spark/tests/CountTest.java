package cz.muni.fi.spark.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.muni.fi.commons.Flow;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.Iterator;

/**
 * Access every message read, convert to json and check if IP matches, if yes, increase counter.
 * Uses object mapper and flow POJO to convert and store JSON messages.
 */
public class CountTest implements Function<JavaPairRDD<String, String>, Void> {
    private static final String FILTERED_IP = "62.148.241.49";
    private static final ObjectMapper mapper = new ObjectMapper();

    private Accumulator<Integer> processedRecordsCounter;
    private Accumulator<Integer> filteredIpCount;

    /**
     * Initializes Count test class with passed Accumulators.
     *
     * @param processedRecordsCounter Accumulator with total of processed records
     * @param filteredIpCount Accumulator with total of IP addresses matched
     */
    public CountTest(Accumulator<Integer> processedRecordsCounter, Accumulator<Integer> filteredIpCount) {
        this.processedRecordsCounter = processedRecordsCounter;
        this.filteredIpCount = filteredIpCount;
    }

    @Override
    public Void call(JavaPairRDD<String, String> rdd) throws IOException {
        rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
            @Override
            public void call(Iterator<Tuple2<String, String>> it) throws IOException {
                Integer tempCount = 0; // all records
                Integer tempFilteredCount = 0; // filtered records
                while (it.hasNext()) {
                    Tuple2<String, String> msg = it.next();
                    Flow flow = mapper.readValue(msg._2(), Flow.class);
                    if (flow.getDst_ip_addr().equals(FILTERED_IP)) {
                        tempFilteredCount++;
                    }
                    tempCount++;
                }
                processedRecordsCounter.add(tempCount);
                filteredIpCount.add(tempFilteredCount);
            }
        });
        return null;
    }
}