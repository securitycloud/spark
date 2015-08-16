package cz.muni.fi.spark.tests;

import cz.muni.fi.kafka.OutputProducer;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.Iterator;

/**
 * Access every message read and send it to Kafka Output.
 * At the end, shared accumulator values are updated from the executor results.
 */
public class ReadWriteTest implements Function<JavaPairRDD<String, String>, Void> {
    private static final OutputProducer prod = new OutputProducer();

    private Accumulator<Integer> processedRecordsCounter;

    /**
     * Initializes ReadWrite test class with passed Accumulator.
     *
     * @param processedRecordsCounter Accumulator with total of processed records
     */
    public ReadWriteTest(Accumulator<Integer> processedRecordsCounter) {
        this.processedRecordsCounter = processedRecordsCounter;
    }

    @Override
    public Void call(JavaPairRDD<String, String> rdd) throws IOException {
        rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
            @Override
            public void call(Iterator<Tuple2<String, String>> it) throws IOException {
                Integer tempCount = 0;
                while (it.hasNext()) {
                    Tuple2<String, String> msg = it.next();
                    prod.send(msg);
                    tempCount++;
                }
                processedRecordsCounter.add(tempCount);
            }
        });
        return null;
    }
}
