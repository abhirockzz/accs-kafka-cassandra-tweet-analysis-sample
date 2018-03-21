package com.oracle.cloud.accs.tweets.producer;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import twitter4j.HashtagEntity;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

public class TwitterStreamListener implements StatusListener {

    @Override
    public void onStatus(Status status) {
        System.out.println("Tweet from @" + status.getUser().getScreenName() + " - " + status.getText());
        List<HashtagEntity> hashtagEntities = Arrays.asList(status.getHashtagEntities());

        if (!hashtagEntities.isEmpty()) {
            System.out.println("Found hashtag in tweet!!!!");
        }
        List<String> hashtags = hashtagEntities.stream().map((t) -> {
            return t.getText();
        }).collect(Collectors.toList());

        if (!status.isPossiblySensitive() && !hashtags.isEmpty()) {
            TweetInfo tweet = new TweetInfo(status.getUser().getScreenName(),
                    status.getText(),
                    status.getCreatedAt(),
                    String.valueOf(status.getId()),
                    hashtags);
            Producer.INSTANCE.produce(tweet);
        } else {
            System.out.println("This tweet will NOT be pushed to Kafka......");
        }

    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
    }

    @Override
    public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
    }

    @Override
    public void onScrubGeo(long userId, long upToStatusId) {
    }

    @Override
    public void onStallWarning(StallWarning warning) {
    }

    @Override
    public void onException(Exception ex) {
        ex.printStackTrace();
    }
}
