package com.oracle.cloud.accs.tweets.processor;

import com.oracle.cloud.accs.tweets.processor.entity.TweetInfo;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * Implements Kafka consumer functionality. This will be executed in a dedicated
 * thread. has been registered as to the JVM runtime shutdown hook
 *
 */
public class KafkaConsumerProcess implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(KafkaConsumer.class.getName());
    private static final String TOPIC_NAME = System.getenv().getOrDefault("OEHCS_TOPIC", "tweets");
    private static final String CONSUMER_GROUP = "tweet-processor-group";
    private final AtomicBoolean CONSUMER_STOPPED = new AtomicBoolean(false);
    private KafkaConsumer<String, TweetInfo> consumer = null;
    private static final String kafkaCluster = System.getenv().getOrDefault("OEHCS_EXTERNAL_CONNECT_STRING", "192.168.99.100:9092");

    /**
     * c'tor
     */
    public KafkaConsumerProcess() {
        Properties kafkaProps = new Properties();
        LOGGER.log(Level.INFO, "Kafka Consumer running in thread {0}", Thread.currentThread().getName());

        LOGGER.log(Level.INFO, "Kafka cluster {0}", kafkaCluster);

        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster);
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "com.oracle.cloud.accs.tweets.processor.ByteToTweetDeserializer");

        this.consumer = new KafkaConsumer<>(kafkaProps);
    }

    /**
     * invoke this to stop this consumer from a different thread
     */
    public void stop() {
        if (CONSUMER_STOPPED.get()) {
            throw new IllegalStateException("Kafka consumer service thread is not running");
        }
        LOGGER.log(Level.INFO, "signalling shut down for consumer");
        if (consumer != null) {
            CONSUMER_STOPPED.set(true);
            consumer.wakeup();
        }

    }

    @Override
    public void run() {
        consume();
    }

    private void consume() {

        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        LOGGER.log(Level.INFO, "Subcribed to: {0}", TOPIC_NAME);
        try {
            while (!CONSUMER_STOPPED.get()) {
                //try-catch within while loop so that conumer loop doesnt exit due to exception
                try {
                    LOGGER.log(Level.INFO, "Polling broker");
                    ConsumerRecords<String, TweetInfo> records = consumer.poll(1000);
                    for (ConsumerRecord<String, TweetInfo> record : records) {
                        TweetInfo tweetInfo = record.value();

                        for (String hashtag : tweetInfo.getHashtags()) {
                            CassandraOperations.persist(tweetInfo, hashtag);
                        }
                    }
                } catch (Exception e) {
                    System.out.println("Could not persist tweet to Cassandra. Consumer app exception - " + e.getMessage());
                }

            }
            LOGGER.log(Level.INFO, "Poll loop interrupted");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
            LOGGER.log(Level.INFO, "consumer shut down complete");
        }

    }

}
