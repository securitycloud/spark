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
 * Access every message read, convert to json and check if IP matches, if yes, then send it to Kafka Output.
 * Uses object mapper and flow POJO to convert and store JSON messages.
 */
public class FilterIPTest implements Function<JavaPairRDD<String, String>, Void> {
    private static final OutputProducer prod = new OutputProducer();
    private static final ObjectMapper mapper = new ObjectMapper();

    private String filterDstIp;
    private Accumulator<Integer> processedRecordsCounter;

    /**
     * Initializes FilterIP test class with passed Accumulator.
     *
     * @param processedRecordsCounter Accumulator with total of processed records
     * @param filterDstIp             destination ip that has to match
     */
    public FilterIPTest(String filterDstIp, Accumulator<Integer> processedRecordsCounter) {
        this.filterDstIp = filterDstIp;
        this.processedRecordsCounter = processedRecordsCounter;
    }

    /**
     * Applies a function call to each partition of this RDD.
     * Function call parses each message on the partition into Flow and if the destination IP of the Flow matches our
     * target FILTERED_IP, we send the message to output kafka topic.
     * At the end updates a count of total processed messages with records processed on the particular node where this
     * method is run.
     *
     * @param rdd batch to be processed
     * @throws IOException on ObjectMapper error
     */
    @Override
    public Void call(JavaPairRDD<String, String> rdd) throws IOException {
        rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
            @Override
            public void call(Iterator<Tuple2<String, String>> it) throws IOException {
                Integer tempCount = 0;
                while (it.hasNext()) {
                    Tuple2<String, String> msg = it.next();
                    Flow flow = mapper.readValue(msg._2(), Flow.class);
                    if (flow.getDst_ip_addr().equals(filterDstIp)) {
                        prod.send(new Tuple2<>(null, flow.toString()));
                    }
                    tempCount++;
                }
                processedRecordsCounter.add(tempCount);
            }
        });
        return null;
    }
}