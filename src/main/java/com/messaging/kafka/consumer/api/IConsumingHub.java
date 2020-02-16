package com.messaging.kafka.consumer.api;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface IConsumingHub<K,V> {
    void listen(String topic, int partition, BiConsumer<K,V> listener);

}
