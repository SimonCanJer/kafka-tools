package com.messaging.kafka.admin.api;

import com.messaging.kafka.InterfaceInstanceFactory;

import java.lang.reflect.Constructor;

public class Provider  extends InterfaceInstanceFactory<IAdminSite> {


    @Override
    protected String getTargetClass() {
        return "com.messaging.kafka.admin.KafkaAdminTools";
    }
}
