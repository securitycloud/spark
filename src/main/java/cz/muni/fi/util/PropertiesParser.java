package cz.muni.fi.util;

import java.io.*;
import java.util.Properties;

/**
 * Created by filip on 16.4.15.
 *
 * Util class for parsing of properties files.
 */
public class PropertiesParser {

    /**
     * Parsers src/main/resources/spark.properties file into java.util.Properties object.
     *
     * @return java.util.Properties parsed properties
     */
    public Properties getSparkProperties() {
        Properties prop = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("spark.properties")) {
            prop.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return prop;
    }

    /**
     * Parsers src/main/resources/kafka.properties file into java.util.Properties object.
     *
     * @return java.util.Properties parsed properties
     */
    public Properties getKafkaProperties() {
        Properties prop = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("kafka.properties")) {
            prop.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return prop;
    }
}
