package cz.muni.fi.spark.tests;

import cz.muni.fi.kafka.OutputProducer;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.Iterator;

/**
 * Testing of R/W only.
 * Access every message read and send it to Kafka Output.
 */
public class ReadWriteTest implements Function<JavaPairRDD<String, String>, Void> {
    private static final OutputProducer prod = new OutputProducer();

    @Override
    public Void call(JavaPairRDD<String, String> rdd) throws IOException {
        rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
            @Override
            public void call(Iterator<Tuple2<String, String>> it) throws IOException {
                while (it.hasNext()) { // for each event in partition
                    Tuple2<String, String> msg = it.next();
                    prod.send(msg);
                }
            }
        });
        return null;
    }
}
