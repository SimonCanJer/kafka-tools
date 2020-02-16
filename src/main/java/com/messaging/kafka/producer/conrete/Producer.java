package com.messaging.kafka.producer.conrete;

import com.messaging.kafka.producer.api.IProducer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Properties;
import java.util.concurrent.Future;

public class Producer<K,V> implements IProducer<K,V> {
    Properties properties = new Properties();
    KafkaProducer<K,V> producer;

    @Override
    public IProducer withBroker(String url, int port) {
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,url+":"+port);
        return this;
    }

    @Override
    public IProducer withId(String sId) {
        properties.put(ProducerConfig.CLIENT_ID_CONFIG,sId);

        return this;
    }

    @Override
    public IProducer withSerializers(Class<? extends Serializer<K>> key, Class<? extends Serializer<V>> value) {
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,key.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,value.getName());
        return this;
    }

    @Override
    public IProducer withPartitioneer(Class value) {
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,value.getName());
        return this;
    }

    @Override
    public Future put(String topic, K key, V value) {
       return put(topic, key, value,null);

    }

    @Override
    public Future put(String topic, K key, V value, Callback cb) {
       return put(topic,Integer.MIN_VALUE,key,value,cb);

    }
    @Override
    public Future put(String topic, int partition, K key, V value, Callback cb) {
        if(producer==null)
            init();
        Integer part=(partition==Integer.MIN_VALUE)?null:new Integer(partition);
        ProducerRecord rec= new ProducerRecord(topic,part,key,value);
        return producer.send(rec,cb);

    }

    public void init()
    {
        if(producer==null)
        {
            synchronized(this)
            {
                if(producer==null)
                {
                    producer= new KafkaProducer<K, V>(properties);
                }
            }
        }
    }
}
