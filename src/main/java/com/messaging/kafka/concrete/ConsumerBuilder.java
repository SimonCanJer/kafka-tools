package com.messaging.kafka.concrete;

import com.messaging.kafka.consumer.api.IConsumerBuilder;
import com.messaging.kafka.consumer.api.IKafkaConsumerPoll;

class ConsumerBuilder implements IConsumerBuilder {
    private ConsumerBuilder()
    {

    }
    @Override
    public <K, V> IKafkaConsumerPoll<K, V> consumerPoll(String name, Class<K> keySerializer, Class<V> valueSerializer) {
        return new KafkaConsumerPoll<>().withGroupName(name).usingSerializers(keySerializer,valueSerializer);
    }

    @Override
    public <K, V> IKafkaConsumerPoll<K, V> consumerPoll(String name, Class<K> keySerializer, Class<V> valueSerializer, String[] qnames) {
        return new KafkaConsumerPoll<>().withGroupName(name).usingSerializers(keySerializer,valueSerializer).basingOnQueues(qnames);
    }
}
