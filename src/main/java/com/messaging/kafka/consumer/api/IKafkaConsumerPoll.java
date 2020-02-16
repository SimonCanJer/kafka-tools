package com.messaging.kafka.consumer.api;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.function.Consumer;


public interface IKafkaConsumerPoll<K,V> {
    IKafkaConsumerPoll basingOnQueues(String[] queues);
    IKafkaConsumerPoll addUrlPort(String url,int port);
    IKafkaConsumerPoll withGroupName(String groupName);
    IKafkaConsumerPoll usingSerializers(Class<? extends Deserializer<K>> serK, Class<? extends Deserializer<V>> ser);
    IKafkaConsumerPoll involvingPercentOfProcessors(float f);
    void runWithConsumer(Consumer<ConsumerRecord<K,V>> processor);
    void run();
    IConsumingHub<K,V> getConsumingHub();
    void shutdown();
}
