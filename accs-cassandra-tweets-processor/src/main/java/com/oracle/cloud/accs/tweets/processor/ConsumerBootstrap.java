package com.oracle.cloud.accs.tweets.processor;


import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;


public final class ConsumerBootstrap {

    private static final Logger LOGGER = Logger.getLogger(ConsumerBootstrap.class.getName());
  
    private static void bootstrap() throws IOException {

        KafkaConsumerProcess kafkaConsumer = new KafkaConsumerProcess(); //will initiate connection to Kafka broker
        
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
               LOGGER.log(Level.INFO, "Exiting......");
                try {

                    kafkaConsumer.stop();
                    LOGGER.log(Level.INFO, "Kafka consumer thread stopped");
                    
                    CassandraOperations.close();
                } catch (Exception ex) {
                    //log & continue....
                    LOGGER.log(Level.SEVERE, ex, ex::getMessage);
                }

            }
        }));
        
        new Thread(kafkaConsumer).start();
        
        CassandraOperations.init(); //establish Cassandra connection and create keyspace, table (if already doesnt exist)

    }

    public static void main(String[] args) throws Exception {

        bootstrap();

    }
}
