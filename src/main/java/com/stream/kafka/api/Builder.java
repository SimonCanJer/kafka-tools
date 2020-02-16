package com.stream.kafka.api;

import com.messaging.kafka.InterfaceInstanceFactory;
import com.messaging.kafka.consumer.api.IConsumerBuilder;

public class Builder extends InterfaceInstanceFactory<IStreamFactoryBuilder> {
    @Override
    protected String getTargetClass() {
        return "com.stream.kafka.impl.StreamFactoryBuilder";
    }
}
