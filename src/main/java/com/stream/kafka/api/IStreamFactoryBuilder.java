package com.stream.kafka.api;

import org.apache.kafka.common.serialization.Serde;

import java.util.function.Supplier;

public interface IStreamFactoryBuilder {
    Supplier<IStreamFactoryBuilder> PROVIDER=new Builder().factory();
    public class ExceptionMissedDef extends RuntimeException
    {

    }
    <K,V> IDefinition<K,V> defineStream(String appName);
    interface IDefinition<K,V> {
        IDefinition usingUrlPort(String url, int port);

        IDefinition usingKeyValueSerdes(Serde<K> keySerde,  Serde<V> valueSerde);
        IDefinition usingStateFolder(String stateFolder);
        IDefinition consumingTopic(String topic);
        IStreamFactory<K,V> create();
    }

}
