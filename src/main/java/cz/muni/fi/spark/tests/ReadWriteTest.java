package cz.muni.fi.spark.tests;

import cz.muni.fi.kafka.OutputProducer;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.Iterator;

/**
 * Access every message read and send it to Kafka Output.
 * At the end, shared accumulator values are updated from the executor results.
 */
public class ReadWriteTest implements VoidFunction<JavaPairRDD<String, String>> {
    private static final OutputProducer prod = new OutputProducer();

    private LongAccumulator processedRecordsCounter;

    /**
     * Initializes ReadWrite test class with passed Accumulator.
     *
     * @param processedRecordsCounter Accumulator with total of processed records
     */
    public ReadWriteTest(LongAccumulator processedRecordsCounter) {
        this.processedRecordsCounter = processedRecordsCounter;
    }

    /**
     * Applies a function call to each partition of this RDD.
     * Function call sends each message on the partition to output kafka topic.
     * At the end updates a count of total processed messages with records processed on the particular node where this
     * method is run.
     *
     * @param rdd batch to be processed
     */
    @Override
    public void call(JavaPairRDD<String, String> rdd) {
        rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
            @Override
            public void call(Iterator<Tuple2<String, String>> it) {
                Integer tempCount = 0;
                while (it.hasNext()) {
                    Tuple2<String, String> msg = it.next();
                    prod.send(msg);
                    tempCount++;
                }
                processedRecordsCounter.add(tempCount);
            }
        });
    }
}
