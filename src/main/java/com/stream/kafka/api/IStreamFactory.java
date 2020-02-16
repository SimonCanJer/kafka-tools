package com.stream.kafka.api;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StreamPartitioner;

import java.io.Serializable;
import java.util.function.Consumer;
import java.util.function.Function;


public interface IStreamFactory<K,V> {
    KStream<K,V> run();
    <K1,V1> KStream<K1,V1> createTransform(Function<KStream<K,V>, KStream<K1,V1>>transformer);
    KStream<K,V> createConsumer(Consumer<KStream<K,V>> consumer);
    void createProcess(Consumer<KStream<K,V>> consumer, ProcessorSupplier<K,V> suppler,String...args);
    <K1, V1> void createTransformProcess(Function<KStream<K,V>,KStream<K1,V1>> consumer, ProcessorSupplier<K1,V1> suppler,String...args);
    <K1, V1> void createTransformRedirect(Function<KStream<K,V>,KStream<K1,V1>> tranform, String topic, Serde<K1> keySerde, Serde<V1> valSerde, StreamPartitioner<K1,V1> partitioner);
    <K1,V1,KM,VA> KTable<KM,VA> apply(Function<KStream<K,V>,KStream<K1,V1>> tranfromer, KeyValueMapper<K1,V1,KM> mapper, Aggregator<KM,V1,VA> aggregator,Initializer<VA> init);
     <K1,V1,KM>    KTable<KM,V1> apply(Function<KStream<K,V>,KStream<K1,V1>> tranfromer,KeyValueMapper<K1,V1,KM> mapper,Reducer<V1> reducer, Serde<KM> keySerde,Serde<V1> valueSerde);
    <K1,V1,KM>    void  mapReduceTo(Function<KStream<K,V>,KStream<K1,V1>> tranfromer,KeyValueMapper<K1,V1,KM> mapper,Reducer<V1> reducer, Serde<KM> keySerde,Serde<V1> valueSerde,String topic);
      <Key extends Serializable, Measure extends Serializable,VT extends Combined<Key,Measure>,I extends Iterable<VT>,KM>    KTable<KM,VT> mapReduceRedirect(ValueMapper<V,I> mapper, KeyValueMapper<K,VT,KM> grouper, Reducer<VT> reducer, Serde<KM> keySerde, Serde<VT> valueSerde, String toTopic);
      void close();
}
