package com.sample.vivek.kafka.learning.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * KafkaProducerWithCallback class which Produces the messages send for Kafka Consumers and it has a callback
 * mechanism where we get the status and message produced to which topic , partition and its offset etc...
 *
 * @author - Vivek Kumar Gupta
 *
 */
public class KafkaProducerWithCallback {

    private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";

    // LOGGER
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerWithCallback.class);

    public static void main(String[] args) {
        // Properties for Producer
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // Create kafka Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for(int i =0 ; i < 1000 ; i++)
        {
            // Create kafka Records
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world "+ i);

            //Send the data
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Received new Metadata. \n " +
                                "Topic : " + recordMetadata.topic() + "\n" +
                                "Partitions : " + recordMetadata.partition() + "\n" +
                                "Offset : " + recordMetadata.offset() + "\n" +
                                "Timestamp : " + recordMetadata.timestamp());

                    } else {
                        logger.error("Error while producing record ", e);
                    }
                }
            });
        }


        // Flush the Producer
        producer.flush();

        // flush ad close the producer
        producer.close();
    }
}
