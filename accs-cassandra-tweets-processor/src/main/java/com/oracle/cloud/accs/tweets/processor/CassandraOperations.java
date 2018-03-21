package com.oracle.cloud.accs.tweets.processor;

import com.oracle.cloud.accs.tweets.processor.entity.TweetInfo;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;

import com.google.common.util.concurrent.ListenableFuture;
import com.oracle.cloud.accs.tweets.processor.entity.HashtagsByDate;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CassandraOperations {

    private static ExecutorService pool = Executors.newSingleThreadExecutor();

    private static final String KEYSPACE = "tweetspace";
    private static final String CREATE_KEYSPACE_QUERY = "CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = {'class':'SimpleStrategy','replication_factor':1};";
    private static final String CREATE_TABLE_QUERY = "CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".hashtags_by_date ( tweeter text, tweet_id text, tweet text, created_date text, hashtag text, PRIMARY KEY (hashtag, created_date, tweet_id)) WITH CLUSTERING ORDER BY (created_date DESC);";

    //cassandra
    private static Session session = null;
    private static Cluster cluster = null;
    private static MappingManager manager = null;

    //Mapper is thread safe
    static Mapper<HashtagsByDate> hashtagsByDateMapper = null;

    static void init() {
        String username = System.getenv().getOrDefault("DHCS_USER_NAME", "admin");
        String password = System.getenv().getOrDefault("DHCS_USER_PASSWORD", "tops3cr3t");
        String cassandraHost = System.getenv().getOrDefault("DHCS_NODE_LIST", "192.168.99.100");
        String cassandraPort = System.getenv().getOrDefault("DHCS_CLIENT_PORT", "9042");

        cluster = Cluster.builder()
                .addContactPoints(cassandraHost.split(",")) //in case of a cluster, nodes will be comma-separated
                .withPort(Integer.valueOf(cassandraPort))
                .withCredentials(username, password)
                .build();

        session = cluster.connect();
        System.out.println("Connected to Cassandra....");

        session.execute(CREATE_KEYSPACE_QUERY);
        System.out.println("Keyspace created...");

        session.execute(CREATE_TABLE_QUERY);
        System.out.println("Table created...");

        manager = new MappingManager(session);
        hashtagsByDateMapper = manager.mapper(HashtagsByDate.class);

    }

    static void persist(TweetInfo tweetInfo, String hashtag) {

        HashtagsByDate hashtagsByDate = new HashtagsByDate(tweetInfo, hashtag);
        ListenableFuture<Void> saveAsync = hashtagsByDateMapper.saveAsync(hashtagsByDate);
        saveAsync.addListener(new Runnable() {
            @Override
            public void run() {
                System.out.println("Details inserted in table " + hashtagsByDate);
            }
        }, pool);
    }

    static void close() {
        session.close();
        System.out.println("Closed connection to Cassandra");
    }

}
