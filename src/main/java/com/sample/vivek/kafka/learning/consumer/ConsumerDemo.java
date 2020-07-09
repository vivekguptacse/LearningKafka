package com.sample.vivek.kafka.learning.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * Consumer Demo class which consumes the messages send from Kafka Producers.
 * @author - Vivek Kumar Gupta
 *
 */
public class ConsumerDemo {

    /**
     * Logger Instance
     */
    private static Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
    /** Bootstrap server */
    private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    /** Topic name */
    private static final String TOPIC = "first_topic";
    /** Group ID */
    private static final String GROUP_ID = "my_second_application";


    public static void main(String[] args) {
        // Create the properties for Consumer
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //Subsribe the topic
        consumer.subscribe(Arrays.asList(TOPIC));  // Can subscribe to any number of topics.

        //Poll the messages

        while(true)
        {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String, String> record : records)
            {
                logger.info("Keys : " + record.key() + " , values : " + record.value());
                logger.info("Partitions : " + record.partition() + " , offset : " + record.offset());
            }
        }

    }
}

