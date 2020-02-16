package com.messaging.kafka.admin.api;

import org.apache.kafka.common.KafkaFuture;

import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;

public interface IAdminSite {
    Supplier<IAdminSite> factory = new Provider().factory();
    IAdminSite with(Properties props);
    IAdminSite withUrlAndPort(String url, int port);
    IAdminSite construct();
    ITopicBatchBuild withTopicBatchBuilder();
    KafkaFuture<Set<String>>  availableTopicNames();
}
