package cz.muni.fi.kafka;

import cz.muni.fi.util.PropertiesParser;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka producer.
 *
 * Created by filip on 23.5.15.
 */
public class OutputProducer {

    private KafkaProducer<String, String> producer;
    private Properties kafkaProps;

    public OutputProducer() {
        kafkaProps = PropertiesParser.getKafkaProperties();
        producer = new KafkaProducer<>(getConfig());
    }

    /**
     * Sends a key-value message to Kafka.
     *
     * @param msg Tuple2 to be send
     */
    public void send(Tuple2<String, String> msg) {
        ProducerRecord<String, String> pr = new ProducerRecord<>(kafkaProps.getProperty("producer.topic")/*, 0,*/ /*msg._1()*/, msg._2());
        try {
            producer.send(pr); // producer is null
            System.out.println("produced to kafka: "+pr);
        } catch (NullPointerException ex) {
            System.out.println(ex);
        }
    }

    /**
     * Sends a json map using toString().
     *
     * @param msg Tuple2 to be send
     */
    public void sendJson(Tuple2<String, Map> msg) {
        ProducerRecord<String, String> pr = new ProducerRecord<>(kafkaProps.getProperty("producer.topic")/*, 0,*/ /*msg._1()*/, msg._2().toString());
        try {
            producer.send(pr); // producer is null
            System.out.println("produced to kafka: "+pr);
        } catch (NullPointerException ex) {
            System.out.println(ex);
        }
    }

    /**
     * Sets producer specific properties that are passed in KafkaProducer constructor.
     * Documentation http://kafka.apache.org/documentation.html#newproducerconfigs
     *
     * @return Map of ProducerConfig properties
     */
    private Map<String, Object> getConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProps.getProperty("bootstrap.servers"));
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, kafkaProps.getProperty("value.serializer"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, kafkaProps.getProperty("key.serializer"));
        props.put(ProducerConfig.CLIENT_ID_CONFIG, kafkaProps.getProperty("client.id"));
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, kafkaProps.getProperty("batch.size"));
        return props;
    }

}
