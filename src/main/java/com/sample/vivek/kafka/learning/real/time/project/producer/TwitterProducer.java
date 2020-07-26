package com.sample.vivek.kafka.learning.real.time.project.producer;

import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.sample.vivek.kafka.learning.bootstrap.BootstrapModule;
import com.sample.vivek.kafka.learning.config.ConfigProperties;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * The class creates the twitter client which reads the twitter sreaming data and sends those data to
 * kafka topics.
 *
 * @author : Vivek Kumar Gupta
 * @since : 20/07/20
 */
public class TwitterProducer {

    private static final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    /**
     * Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream
     */
    private BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

    private ConfigProperties configProperties;

    @Inject
    public TwitterProducer(ConfigProperties configProerties) {
        this.configProperties = configProerties;
    }


    public static void main(String[] args) {

        Injector injector = Guice.createInjector(new BootstrapModule());
        TwitterProducer twitterProducer = injector.getInstance(TwitterProducer.class);
        twitterProducer.run();

    }

    public void run() {
        //Create Twitter Client
        Client client = createTwitterClient();

        // Attempts to establish a connection.
        client.connect();


        // Create Kafka Producer

        // Send Messages to kafka.

        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            logger.info(msg);

        }

        logger.info("Exiting the Application...");
    }


    public Client createTwitterClient() {

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        List<String> terms = Lists.newArrayList("spark");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(configProperties.getConsumer_key(), configProperties.getConsumer_secret(), configProperties.getToken(), configProperties.getSecret());

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")               // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();

        return hosebirdClient;
    }

}
