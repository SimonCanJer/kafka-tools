package com.messaging.kafka.producer.api;

import com.messaging.kafka.InterfaceInstanceFactory;

public class ProducerFactory extends InterfaceInstanceFactory<IProducerFactory> {
    @Override
    protected String getTargetClass() {
        return "com.messaging.kafka.producer.conrete.ProducerBuilder";
    }
}
