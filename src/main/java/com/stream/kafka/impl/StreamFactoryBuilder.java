package com.stream.kafka.impl;

import com.stream.kafka.api.IStreamFactory;
import com.stream.kafka.api.IStreamFactoryBuilder;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class StreamFactoryBuilder implements IStreamFactoryBuilder {
    private StreamFactoryBuilder(){};
    @Override
    public <K, V> IDefinition<K, V> defineStream(String appName) {
        return new Definition<>(appName);
    }
    static private class Definition<K,V> implements IDefinition<K,V>
    {

        Properties configuration = new Properties();
        StringBuilder builder = new StringBuilder();
        String theTopic;
        private Serde<K> kSerde;
        private Serde<V> vSerde;



        Definition(String name)
        {
            String myDir= System.getProperty("user.dir");
            configuration.put(StreamsConfig.APPLICATION_ID_CONFIG,name);
           /// configuration.put(StreamsConfig.STATE_DIR_CONFIG,myDir);
        }

        @Override
        public IDefinition usingUrlPort(String url, int port) {
            if(builder.length()>0)
                builder.append(",");
            builder.append(url).append(':').append(port);
            return this;
        }

        @Override
        public IDefinition usingKeyValueSerdes(Serde<K> keySerde, Serde<V> valueSerde) {
            this.kSerde=keySerde;
            this.vSerde=valueSerde;
            configuration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,keySerde.getClass().getName());
            configuration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,valueSerde.getClass().getName());
            return this;

        }



        @Override
        public IDefinition usingStateFolder(String stateFolder) {
            configuration.put(StreamsConfig.STATE_DIR_CONFIG,stateFolder);
            return null;
        }

        @Override
        public IDefinition consumingTopic(String topic) {
            theTopic = topic;
            return this;
        }

        @Override
        public IStreamFactory<K, V> create() {
            if(configuration.size()<3||builder.length()==0|| theTopic==null)
            {
                throw new  ExceptionMissedDef();
            }
            configuration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,builder.toString());
            return new StreamFactory<>(configuration,theTopic) ;
        }
    }
}
