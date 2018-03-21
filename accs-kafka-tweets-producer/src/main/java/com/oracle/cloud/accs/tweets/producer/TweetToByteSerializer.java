package com.oracle.cloud.accs.tweets.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.common.serialization.Serializer;

public class TweetToByteSerializer implements Serializer<TweetInfo>{
    
    static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> map, boolean bln) {
        //no-op
    }

    @Override
    public byte[] serialize(String topic, TweetInfo t) {
        byte[] serializedTweetInfo = null;
        try {
            serializedTweetInfo = MAPPER.writeValueAsBytes(t);
            //System.out.println("Serialized tweet "+ t + " to bytes");
        } catch (JsonProcessingException ex) {
            Logger.getLogger(TweetToByteSerializer.class.getName()).log(Level.SEVERE, null, ex);
        }
        return serializedTweetInfo;
    }

    @Override
    public void close() {
        //no-op
    }
    
}
