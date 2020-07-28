package learning.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * KafkaProducerWithKeys class produces to message with keys, so with that keys a message will always
 * goes to same partition.
 * @author  Vivek Kumar Gupta
 *
 */
public class KafkaProducerWithKeys {

    private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";

    // LOGGER
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerWithKeys.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // Properties for Producer
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // Create kafka Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        String topic = "first_topic";

        for(int i =0 ; i < 10 ; i++)
        {
            String key = "id_" + i;
            String value = "Hello World " + i;
            // Create kafka Records
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            logger.info("Key : " + key);

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
            }).get();  // Block the send to make it synchronous, Dont do this in production !!
        }


        // Flush the Producer
        producer.flush();

        // flush ad close the producer
        producer.close();
    }
}
