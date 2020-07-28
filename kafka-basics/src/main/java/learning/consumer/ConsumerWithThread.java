package learning.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * Class which will Check the behavior of ConsuerRunnable thread class
 *
 * @author : Vivek Kumar Gupta
 * @since : 18/07/20
 */
public class ConsumerWithThread {

    /**
     * Logger Instance
     */
    private static Logger logger = LoggerFactory.getLogger(ConsumerWithThread.class);
    /**
     * Bootstrap server
     */
    private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    /**
     * Topic name
     */
    private static final String TOPIC = "first_topic";
    /**
     * Group ID
     */
    private static final String GROUP_ID = "my_second_application";


    public ConsumerWithThread() {

    }

    public void run() {
        logger.info("Creating the consumer runnable. ");
        CountDownLatch latch = new CountDownLatch(1);
        Runnable myConsumerRunnable = new ConsumerRunnable(BOOTSTRAP_SERVER, TOPIC, GROUP_ID, latch);
        logger.info("Strats the thread.");
        Thread consumerThread = new Thread(myConsumerRunnable);
        consumerThread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook.");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited.");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application is inturrupted. ", e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public static void main(String[] args) {
        ConsumerWithThread consumerWithThread = new ConsumerWithThread();
        consumerWithThread.run();


    }
}
