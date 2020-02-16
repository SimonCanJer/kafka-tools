package com.messaging.kafka.consumer.api;

import java.util.function.Supplier;

public interface IConsumerBuilder {
    Supplier<IConsumerBuilder> theBuilder = new Builder().factory();
    <K,V> IKafkaConsumerPoll<K,V> consumerPoll(String name,Class<K> keySerializer, Class<V> valueSerializer);
    <K,V> IKafkaConsumerPoll<K,V> consumerPoll(String name,Class<K> keySerializer, Class<V> valueSerializer, String[] qnames);
}
