package cz.muni.fi.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.muni.fi.json.Flow;
import cz.muni.fi.json.JSONFlattener;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Netcat streaming test.
 *
 * @author Martin Jelínek (xjeline5)
 */
public class JsonStreamingMain {

    private static final Pattern SPACE = Pattern.compile(" ");
    public static int zeroCount = 0;

    public static void main(String[] args) {
        final SparkConf conf = SparkConfProducer.getSparkConf();
        final JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(1));
        final JavaReceiverInputDStream<String> lines = ssc.socketTextStream("localhost", 9999, StorageLevels.MEMORY_AND_DISK_SER);

        //long startTime = System.currentTimeMillis();

        //JavaDStream<Flow> flows = mapFlows(lines, false);
        //flows.count().foreachRDD(new Function<JavaRDD<Long>, Void>() {

        JavaDStream<Map<String, Object>> jsonMap = mapJson(lines, false);
        jsonMap.count().foreachRDD(rdd -> {
            final Long count = rdd.first();
            System.out.println(count + " lines");
            if (count == 0) {
                zeroCount++;
                if (zeroCount == 5) {
                    ssc.stop(true, true);
                    System.exit(zeroCount);
                }
            }
            return null;
        });
        //System.out.println(flows.count() + " lines");
        //System.out.println((System.currentTimeMillis() - startTime) / 1000d + " s");

        ssc.start();
        ssc.awaitTermination();
    }

    private static JavaDStream<Flow> mapFlows(JavaReceiverInputDStream<String> lines, final boolean print) {
        JavaDStream<Flow> result = lines.mapPartitions(new FlatMapFunction<Iterator<String>, Flow>() {
            @Override
            public Iterable<Flow> call(Iterator<String> lines) throws Exception {
                ArrayList<Flow> flows = new ArrayList<>();
                ObjectMapper mapper = new ObjectMapper();
                while (lines.hasNext()) {
                    String line = lines.next();
                    try {
                        flows.add(mapper.readValue(line, Flow.class));
                    } catch (Exception e) {
                        // skip records on failure
                        System.out.println(e.toString());
                    }
                }
                return flows;
            }
        });
        result.foreach(new Function<JavaRDD<Flow>, Void>() {
            @Override
            public Void call(JavaRDD<Flow> v1) throws Exception {
                v1.foreach(new VoidFunction<Flow>() {

                    @Override
                    public void call(Flow arg0) throws Exception {
                        if (print) {
                            System.out.println(arg0.toString());
                        }
                    }
                });
                return null;
            }
        });
        return result;
    }

    private static JavaDStream<Map<String, Object>> mapJson(JavaReceiverInputDStream<String> lines, boolean print) {
        JavaDStream<Map<String, Object>> result = lines.mapPartitions(new FlatMapFunction<Iterator<String>, Map<String, Object>>() {
            @Override
            public Iterable<Map<String, Object>> call(Iterator<String> lines) throws Exception {
                List<Map<String, Object>> jsonList = new ArrayList<>();
                JSONFlattener jsonFlattener = new JSONFlattener(new ObjectMapper());
                while (lines.hasNext()) {
                    String line = lines.next();
                    try {
                        Map<String, Object> jsonToFlatMap = jsonFlattener.jsonToFlatMap(line);
                        jsonList.add(jsonToFlatMap);
                    } catch (Exception e) {
                        // skip records on failure
                    }
                }
                return jsonList;
            }
        });
        if (print) {
            result.foreach(new Function<JavaRDD<Map<String, Object>>, Void>() {
                @Override
                public Void call(JavaRDD<Map<String, Object>> v1) throws Exception {
                    v1.foreach(new VoidFunction<Map<String, Object>>() {

                        @Override
                        public void call(Map<String, Object> arg0) throws Exception {
                            System.out.println(arg0.toString());
                        }
                    });
                    return null;
                }
            });
        }
        return result;
    }


}
