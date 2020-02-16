package com.messaging.kafka.admin.api;

import org.apache.kafka.common.KafkaFuture;

public interface ITopicBatchBuild {

    ITopicBatchBuild add(String name, int partitions,short replications);
    KafkaFuture<Void> commit();

}
