package cz.muni.fi.spark;

import cz.muni.fi.util.PropertiesParser;
import org.apache.spark.SparkConf;

import java.util.Properties;

/**
 * Created by filip on 23.5.15.
 */
public class ConfigurationProducer {
    public static SparkConf getSparkConf() {
        final Properties sparkProps = new PropertiesParser().getSparkProperties();
        return new SparkConf()
                .setSparkHome(sparkProps.getProperty("spark.home"))
                .setAppName(sparkProps.getProperty("spark.app.name"))
                .set("spark.ui.port", sparkProps.getProperty("spark.ui.port"))
                .setMaster(sparkProps.getProperty("spark.master.url"))
                .set("spark.executor.memory", sparkProps.getProperty("spark.executor.memory"))
                .set("spark.serializer", sparkProps.getProperty("spark.serializer"));
    }
}
