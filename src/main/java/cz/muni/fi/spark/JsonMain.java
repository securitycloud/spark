package cz.muni.fi.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.scala.DefaultScalaModule;
import cz.muni.fi.json.Flow;
import cz.muni.fi.json.JSONFlattener;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Testing of object mapper on a whole file without Spark streaming.
 *
 * @author Martin Jelínek (xjeline5)
 */
public class JsonMain {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        SparkConf conf = ConfigurationProducer.getSparkConf();//new SparkConf().setAppName("JsonTest").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        ObjectMapper mapper = new ObjectMapper(); //with ScalaObjectMapper
        mapper.registerModule(new DefaultScalaModule());

        //JavaRDD<String> lines = sc.textFile("d:/Martin/school/DP/out_small");
        JavaRDD<String> lines = sc.textFile("/home/filip/thesis/sample.json");
        //SQLContext sqlContext = new SQLContext(sc);
        long startTime = System.currentTimeMillis();

        // mapping with pojo
        JavaRDD<Flow> flows = mapFlows(lines, false);
        System.out.println(flows.count() + " lines");
        System.out.println("Duration of mapping using flows: "+ (System.currentTimeMillis() - startTime) / 1000d + " s");


        // mapping with jsonFlatMap
        /*JavaRDD<Map<String, Object>> jsonMap = mapJson(lines, false);
        System.out.println(jsonMap.count() + " lines");
        System.out.println("Duration of mapping using jsonToFlatMap: "+ (System.currentTimeMillis() - startTime) / 1000d + " s");
        */

        // mapping with Spark SQL module
        /*DataFrame events = sqlContext.jsonFile("/home/filip/thesis/36846558lines.json");
        System.out.println(events.count() + " lines");
        events.registerTempTable("events");
//        events.printSchema();
//        sqlContext.sql("SELECT * FROM events").show();
        System.out.println("Duration of mapping using Spark SQL: " + (System.currentTimeMillis() - startTime) / 1000d + " s");*/
    }

    private static JavaRDD<Flow> mapFlows(JavaRDD<String> lines, boolean print) {
        JavaRDD<Flow> result = lines.mapPartitions(new FlatMapFunction<Iterator<String>, Flow>() {
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

        if (print) {
            result.foreach(new VoidFunction<Flow>() {
                @Override
                public void call(Flow arg0) throws Exception {
                    System.out.println(arg0.toString());
                }
            });
        }
        return result;
    }

    private static JavaRDD<Map<String, Object>> mapJson(JavaRDD<String> lines, boolean print) {
        JavaRDD<Map<String, Object>> result = lines.mapPartitions(new FlatMapFunction<Iterator<String>, Map<String, Object>>() {
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
            result.foreach(new VoidFunction<Map<String, Object>>() {

                @Override
                public void call(Map<String, Object> arg0) throws Exception {
                    System.out.println(arg0.toString());
                }
            });
        }
        return result;
    }

}
