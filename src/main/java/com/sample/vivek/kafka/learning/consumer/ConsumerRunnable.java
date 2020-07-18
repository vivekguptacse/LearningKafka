package com.sample.vivek.kafka.learning.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * This class created Consumer Runnable, which will be used in a
 * multithreaded environment to create consumers.
 *
 * @author : Vivek Kumar Gupta
 * @since : 18/07/20
 */
public class ConsumerRunnable implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

    private KafkaConsumer<String, String> consumer;

    private CountDownLatch latch;

    public ConsumerRunnable(String bootstrap_server, String topic, String groupId, CountDownLatch latch) {
        this.latch = latch;

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));

    }

    @Override
    public void run() {

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Keys : " + record.key() + " , values : " + record.value());
                    logger.info("Partitions : " + record.partition() + " , offset : " + record.offset());
                }
            }

        } catch (WakeupException e) {
            logger.info("Received shutdown signal");
        } finally {
            consumer.close();
            // tell our main code that we are done.
            latch.countDown();
        }
    }

    public void shutdown() {
        // the wakeup method is special method to interrupt consumer.poll()
        // it will throw the exception WakeupException.
        consumer.wakeup();
    }
}
