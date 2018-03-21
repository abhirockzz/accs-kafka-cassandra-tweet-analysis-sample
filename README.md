## Tweet analysis app with Kafka and Cassandra

![](https://cdn-images-1.medium.com/max/1000/1*t3xFDA1vjcibRSBkmA5s8w.png)

**Tweet Producer app**

- It’s a Java app and uses twitter4j library to consume the tweet stream
- Applies user defined filter criteria/terms to filter relevant tweets from the stream
- a custom serializer (`org.apache.kafka.common.serialization.Serializer`) based on the Jackson `ObjectMapper` and pushes it to a Kafka topic
- It provides a REST API to start/stop the app on demand e.g. /tweets/producer

**Tweet processor service**

- a Kafka consumer app which consumes tweets from a Kafka topic
- it is modeled as a worker service within Oracle Application Container Cloud — more on this below
- De-serializes (custom `org.apache.kafka.common.serialization.Deserializer`) converts tweet data from topic into a POJO, and finally
- persists this in-memory Java object to a Cassandra table (called hashtags_by_date)

For more info, check out the [complete blog](tbd)