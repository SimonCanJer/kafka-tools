package com.messaging.kafka.producer.conrete;

import com.messaging.kafka.producer.api.IProducer;
import com.messaging.kafka.producer.api.IProducerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class ProducerBuilder implements IProducerFactory {
    Map<String, IProducer> instances = new HashMap<>();

    private ProducerBuilder() {

    }

    @Override
    public <K, V> IProducer<K, V> createWithId(String id) {

        if (!instances.containsKey(id)) {
            synchronized (this) {
                if (!instances.containsKey(id)) {
                    instances.put(id, new Producer().withId(id));
                }
            }
        }
        return instances.get(id).withId(id);


    }
}
