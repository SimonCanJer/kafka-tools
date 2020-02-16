package com.messaging.kafka.producer.api;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.common.serialization.Serializer;

import java.util.concurrent.Future;

public interface IProducer<KEY, VAL> {
   IProducer withBroker(String url, int port);
   IProducer withId(String sId);

   IProducer withSerializers(Class<? extends Serializer<KEY>> key,Class<? extends Serializer<VAL>> value);
    IProducer withPartitioneer(Class value);
    void init();
    Future put(String topic, KEY key, VAL value);
    Future put(String topic, KEY key, VAL value, Callback cb);
    Future put(String topic, int partition, KEY key, VAL value, Callback cb);
}
