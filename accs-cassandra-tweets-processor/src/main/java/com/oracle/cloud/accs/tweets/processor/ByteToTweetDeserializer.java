package com.oracle.cloud.accs.tweets.processor;

import com.oracle.cloud.accs.tweets.processor.entity.TweetInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.common.serialization.Deserializer;

public class ByteToTweetDeserializer implements Deserializer<TweetInfo>{
    
    static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> map, boolean bln) {
        //no-op
    }

    @Override
    public TweetInfo deserialize(String topic, byte[] bytes) {
        TweetInfo tweet = null;
        try {
            tweet = mapper.readValue(bytes, TweetInfo.class);
            System.out.println("successfully de-serialized bytes to tweet "+ tweet);
        } catch (IOException ex) {
            Logger.getLogger(ByteToTweetDeserializer.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return tweet;
    }

    @Override
    public void close() {
        //no-op
    }
    
}
