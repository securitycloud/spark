package cz.muni.fi.spark;

import cz.muni.fi.kafka.OutputProducer;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.Iterator;

/**
 * Provides nested classes (to be passed instead of anonymous classes to Spark) for testing of R/W only.
 */
public class ReadWriteTest {

    /**
     * Access every message read.
     */
    public static class ReadOnly implements Function<JavaPairRDD<String, String>, Void> {
        @Override
        public Void call(JavaPairRDD<String, String> rdd) throws IOException {
            rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
                @Override
                public void call(Iterator<Tuple2<String, String>> it) throws IOException {
                    while (it.hasNext()) { // for each event in partition
                        Tuple2<String, String> msg = it.next();
                    }
                }
            });
            return null;
        }
    }

    /**
     * Access every message read and send it to Kafka Output.
     */
    public static class ReadWrite implements Function<JavaPairRDD<String, String>, Void> {
        public static final OutputProducer prod = new OutputProducer();
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
}
