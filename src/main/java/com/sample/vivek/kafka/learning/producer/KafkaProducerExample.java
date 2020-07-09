package com.sample.vivek.kafka.learning.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * KafkaProducer class which Produces the messages send for Kafka Consumers.
 * @author - Vivek Kumar Gupta
 *
 */
public class KafkaProducerExample {

    private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";

    public static void main(String[] args) {
        // Create properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        // Create Producer Records
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello World Jhakkas !!! ");

        // Send Record data
        kafkaProducer.send(record);

        // Flush the records -- it will not be send untill we flush or close
        kafkaProducer.flush();

        // flush and close
        kafkaProducer.close();
    }

}
