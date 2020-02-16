package com.messaging.kafka.producer.api;

import java.util.function.Supplier;

public  interface  IProducerFactory {
    Supplier<IProducerFactory> factory = new ProducerFactory().factory();
    <K,V> IProducer<K,V> createWithId(String id);
}
