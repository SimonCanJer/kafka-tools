package com.stream.kafka.impl;

import com.stream.kafka.api.Combined;
import com.stream.kafka.api.IStreamFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

import java.io.Serializable;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Function;

public class StreamFactory<K,V> implements IStreamFactory<K,V>{
    private final Properties config;
    private final StreamsBuilder builder;
    private KafkaStreams kafkaStreams;
    KStream<K,V>    stream;



    StreamFactory(Properties props, String topic)
    {
        config= props;
        ///props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        builder = new StreamsBuilder();
        stream=builder.stream(topic);


    }

    @Override
    public KStream<K, V> run() {
        kafkaStreams = new KafkaStreams(builder.build(),config);
        kafkaStreams.start();
        return stream;
    }

    @Override
    public <K1, V1> KStream<K1, V1> createTransform(Function<KStream<K, V>, KStream<K1, V1>> transformer) {

        try
        {
            return transformer.apply(stream);

        }
        finally
        {

        }
    }

    @Override
    public KStream<K, V> createConsumer(Consumer<KStream<K, V>> consumer) {
        try {
            consumer.accept(stream);
            return stream;
        }
        finally
        {

        }
    }

    @Override
    public void createProcess(Consumer<KStream<K, V>> consumer, ProcessorSupplier<K, V> processor, String... args) {
        try {
            if(consumer!=null)
                consumer.accept(stream);
            stream.process(processor,args);


        }
        finally
        {

        }

    }

    @Override
    public <K1, V1> void createTransformProcess(Function<KStream<K,V>,KStream<K1,V1>> consumer, ProcessorSupplier<K1,V1> suppler,String...args) {
        try {
            consumer.apply(stream).process(suppler,args);
        }
        finally
        {
        }

    }

    @Override
    public <K1, V1> void createTransformRedirect(Function<KStream<K, V>, KStream<K1, V1>> tranform,String topic, Serde<K1> keySerde, Serde<V1> valSerde,StreamPartitioner<K1,V1> partitioner) {
        try {

            tranform.apply(stream).to(topic,Produced.with(keySerde, valSerde));
        }
        finally
        {

        }

    }

    @Override
    public <K1, V1, KM, VA> KTable<KM, VA> apply(Function<KStream<K, V>, KStream<K1, V1>> tranfromer, KeyValueMapper<K1, V1, KM> mapper, Aggregator<KM, V1, VA> aggregator, Initializer<VA> init) {

        try {
            KStream<K1, V1> out = (tranfromer != null) ? tranfromer.apply(stream) : (KStream<K1, V1>) stream;

            return out.groupBy(mapper).aggregate(init, aggregator);
        }
        finally
        {

        }

    }

    @Override
    public <K1, V1, KM> KTable<KM, V1> apply(Function<KStream<K, V>, KStream<K1, V1>> tranfromer, KeyValueMapper<K1, V1, KM> mapper,Reducer<V1> reducer, Serde<KM> keySerde, Serde<V1> valueSerde) {
        try {
            KStream<K1, V1> out = (tranfromer != null) ? tranfromer.apply(stream) : (KStream<K1, V1>) stream;
            return out.groupBy(mapper).reduce(reducer);
        }
        finally
        {

        }
    }

    @Override
    public <K1, V1, KM> void mapReduceTo(Function<KStream<K, V>, KStream<K1, V1>> tranfromer, KeyValueMapper<K1, V1, KM> mapper, Reducer<V1> reducer, Serde<KM> keySerde, Serde<V1> valueSerde, String topic) {
        try {
            KStream<K1, V1> out = (tranfromer != null) ? tranfromer.apply(stream) : (KStream<K1, V1>) stream;
            out.groupBy(mapper).reduce(reducer).toStream().to(topic);
        }
        finally
        {

        }
    }

    @Override
    public <Key extends Serializable, Measure extends Serializable,VT extends Combined<Key,Measure>,I extends Iterable<VT>,KM>    KTable<KM,VT> mapReduceRedirect(ValueMapper<V,I> mapper, KeyValueMapper<K,VT,KM> grouper, Reducer<VT> reducer, Serde<KM> keySerde, Serde<VT> valueSerde, String toTopic)    {

        try
        {


           KTable<KM, VT> res=stream.flatMapValues((ValueMapper<V,Iterable<VT>>) mapper).groupBy(grouper,Serialized.with(keySerde,valueSerde)).reduce(reducer);
           if(toTopic!=null)
               res.toStream().to(toTopic, Produced.with(keySerde, valueSerde));
           return res;

        }
        finally
        {

        }
    }

    @Override
    public void close() {
        kafkaStreams.close();
    }
}
