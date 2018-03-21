package com.oracle.cloud.accs.tweets.producer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class TweetInfo {

    private String tweeter;
    private String tweet;
    //private Date created;
    private String created_date;
    private String tweet_id;
    private List<String> hashtags;
    
    public TweetInfo() {
    }

    public TweetInfo(String tweeter, String tweet, Date created, String tweet_id, List<String> hashtags) {
        this.tweeter = tweeter;
        this.tweet = tweet;
        //this.created = created;
        this.created_date = new SimpleDateFormat("yyyy-MM-dd").format(created);
        this.tweet_id = tweet_id;
        this.hashtags = hashtags;
    }

    public String getTweeter() {
        return tweeter;
    }

    public String getTweet() {
        return tweet;
    }

//    public Date getCreated() {
//        return created;
//    }

    public String getTweet_id() {
        return tweet_id;
    }

    public String getCreated_date() {
        return created_date;
    }

    public List<String> getHashtags() {
        return hashtags;
    }

    @Override
    public String toString() {
        return "TweetInfo{" + "tweeter=" + tweeter + ", tweet=" + tweet + ", created_date=" + created_date + ", tweet_id=" + tweet_id + ", hashtags=" + hashtags + '}';
    }


    

}
