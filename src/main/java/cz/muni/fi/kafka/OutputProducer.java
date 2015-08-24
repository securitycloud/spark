package cz.muni.fi.kafka;

import cz.muni.fi.util.PropertiesParser;
import kafka.javaapi.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka producer.
 */
public class OutputProducer {

    private KafkaProducer<String, String> producer;
    private Properties kafkaProps;

    public OutputProducer() {
        kafkaProps = PropertiesParser.getKafkaProperties();
        producer = new KafkaProducer<>(getConfig());
    }

    /**
     * Sends a key value pair to Kafka, key is null, value is string.
     *
     * @param msg Tuple2 to be send
     */
    public void send(Tuple2<String, String> msg) {
        producer.send(new ProducerRecord<>(kafkaProps.getProperty("producer.topic"), null, msg._2()));
    }

    /**
     * Sends a key value pair to Kafka, key is null, value is string.
     *
     * @param msg   Tuple2 to be send
     * @param topic overrides the default output topic
     */
    public void send(Tuple2<String, String> msg, String topic) {
        producer.send(new ProducerRecord<>(topic, null, msg._2()));
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
