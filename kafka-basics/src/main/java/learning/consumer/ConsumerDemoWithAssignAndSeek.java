package learning.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * Consumer Demo class which consumes the messages send from Kafka Producers with a particular partition
 * and after defined offset.
 *
 * @author : Vivek Kumar Gupta
 * @since : 18/07/20
 */
public class ConsumerDemoWithAssignAndSeek {

    /**
     * Logger Instance
     */
    private static Logger logger = LoggerFactory.getLogger(ConsumerDemoWithAssignAndSeek.class);
    /**
     * Bootstrap server
     */
    private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    /**
     * Topic name
     */
    private static final String TOPIC = "first_topic";

    public static void main(String[] args) {
        // Create the properties for Consumer
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // Assign and seek are mostly used to replay data or fetch a specific message.

        // Assign
        TopicPartition topicPartitionToReadFrom = new TopicPartition(TOPIC, 0);
        long offset = 15L;
        consumer.assign(Arrays.asList(topicPartitionToReadFrom));

        // Seek
        consumer.seek(topicPartitionToReadFrom, offset);

        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberofMessagesReadSoFar = 0;

        //Poll the messages


        while (keepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                numberofMessagesReadSoFar += 1;
                logger.info("Keys : " + record.key() + " , values : " + record.value());
                logger.info("Partitions : " + record.partition() + " , offset : " + record.offset());
                if (numberofMessagesReadSoFar > numberOfMessagesToRead) {
                    keepOnReading = false; // to exit while loop
                    break;   // to exit for loop.

                }
            }

            logger.info("Exiting the Application.");
        }

    }
}

