package com.oracle.cloud.accs.tweets.processor.entity;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table(keyspace = "tweetspace", name = "hashtags_by_date",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM",
        caseSensitiveKeyspace = false,
        caseSensitiveTable = false)
public class HashtagsByDate {

    @PartitionKey
    private String hashtag;

    @Column
    private String created_date;
    
    @Column
    private String tweet_id;
    
    private String tweeter;
    private String tweet;

    public HashtagsByDate() {
    }

    public HashtagsByDate(TweetInfo tweetInfo, String hashtag) {
        this.tweeter = tweetInfo.getTweeter();
        this.tweet = tweetInfo.getTweet();
        this.created_date = tweetInfo.getCreated_date();
        this.tweet_id = tweetInfo.getTweet_id();
        this.hashtag = hashtag;
    }

    public String getTweeter() {
        return tweeter;
    }

    public String getTweet() {
        return tweet;
    }

    public String getTweet_id() {
        return tweet_id;
    }

    public String getCreated_date() {
        return created_date;
    }

    public String getHashtag() {
        return hashtag;
    }

    @Override
    public String toString() {
        return "HashtagsByDate{" + "hashtag=" + hashtag + ", created_date=" + created_date + ", tweet_id=" + tweet_id + ", tweeter=" + tweeter + ", tweet=" + tweet + '}';
    }

}
