package com.messaging.kafka.consumer.api;

import com.messaging.kafka.InterfaceInstanceFactory;

public class Builder extends InterfaceInstanceFactory<IConsumerBuilder> {

    @Override
    protected String getTargetClass() {
        return "com.messaging.kafka.concrete.ConsumerBuilder";
    }
}
