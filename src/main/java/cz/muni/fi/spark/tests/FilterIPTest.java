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
 * Access every message read, convert to json and check if IP matches, if yes, increase filter counter
 * Uses object mapper and flow POJO to convert and store JSON messages.
 */
public class FilterIPTest implements Function<JavaPairRDD<String, String>, Void> {
    private static final ObjectMapper mapper = new ObjectMapper();

    private final String filterSrcIp;
    private final Accumulator<Integer> processedRecordsCounter;
    private final Accumulator<Integer> filteredIpCount;
    

    /**
     * Initializes FilterIP test class with passed Accumulator.
     *
     * @param filterSrcIp             source IP that has to match
     * @param processedRecordsCounter Accumulator with total of processed records
     * @param filteredIpCount Accumulator with total of IP addresses matched
     */
    public FilterIPTest(String filterSrcIp, Accumulator<Integer> processedRecordsCounter,
            Accumulator<Integer> filteredIpCount) {
        this.filterSrcIp = filterSrcIp;
        this.processedRecordsCounter = processedRecordsCounter;
        this.filteredIpCount = filteredIpCount;
    }

    @Override
    public Void call(JavaPairRDD<String, String> rdd) throws IOException {
        rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
            @Override
            public void call(Iterator<Tuple2<String, String>> it) throws IOException {
                Integer tempCount = 0;
                Integer tempFilteredCount = 0;
                while (it.hasNext()) {
                    Tuple2<String, String> msg = it.next();
                    Flow flow = mapper.readValue(msg._2(), Flow.class);
                    if (flow.getSrc_ip_addr().equals(filterSrcIp)) {
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