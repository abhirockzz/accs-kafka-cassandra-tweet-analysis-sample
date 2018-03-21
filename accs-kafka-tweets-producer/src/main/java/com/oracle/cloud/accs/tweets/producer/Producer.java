package com.oracle.cloud.accs.tweets.producer;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class Producer {

    /**
     * Kafka Producer is thread safe. So we can have a singleton
     * https://kafka.apache.org/0102/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html
     */
    private static final Logger LOGGER = Logger.getLogger(Producer.class.getName());

    private static final String TOPIC_NAME = System.getenv().getOrDefault("OEHCS_TOPIC", "tweets");
    private static final String KAFKA_CLUSTER = System.getenv().getOrDefault("OEHCS_EXTERNAL_CONNECT_STRING", "localhost:9092");

    private static KafkaProducer<String, TweetInfo> PRODUCER = null;

    public static final Producer INSTANCE = new Producer();

    private Producer() {

    }

    static {
        LOGGER.log(Level.INFO, "Kafka Producer running in thread {0}", Thread.currentThread().getName());
        Properties kafkaProps = new Properties();

        LOGGER.log(Level.INFO, "Kafka cluster {0}", KAFKA_CLUSTER);

        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        //custom implementation
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "com.oracle.cloud.accs.tweets.producer.TweetToByteSerializer");
        kafkaProps.put(ProducerConfig.ACKS_CONFIG, "0"); //boom!

        PRODUCER = new KafkaProducer<>(kafkaProps);
    }

    /**
     * produce messages
     *
     * @throws Exception
     */
    void produce(TweetInfo tweetInfo) {
        ProducerRecord<String, TweetInfo> record = null;

        try {
            record = new ProducerRecord<>(TOPIC_NAME, tweetInfo.getTweet_id(), tweetInfo);

            PRODUCER.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata rm, Exception excptn) {
                    if (excptn != null) {
                        LOGGER.log(Level.WARNING, "Error sending message with key {0}\n{1}", new Object[]{tweetInfo.getTweet_id(), excptn.getMessage()});
                    } else {
                        LOGGER.log(Level.INFO, "Partition for key {0} is {1}", new Object[]{tweetInfo.getTweet_id(), rm.partition()});
                    }

                }
            });

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Producer thread was interrupted");
        }

    }

    void close() {
        PRODUCER.close();
    }

}
