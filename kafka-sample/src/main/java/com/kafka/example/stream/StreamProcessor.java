package com.kafka.example.stream;

import com.stream.kafka.api.Combined;
import com.stream.kafka.api.CombinedSerde;
import com.stream.kafka.api.IStreamFactory;
import com.stream.kafka.api.IStreamFactoryBuilder;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.*;

public class StreamProcessor {


    static class StringStat  extends Combined<String, Long>
    {

        protected StringStat(String s, Long m) {
            super(s, m);
        }

        protected Long combine(Long measure) {
            return measure()+measure;
        }


    }
    static public void main(String[] args)
    {
      /*  Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream("test");
        KTable<String, Long> wordCounts = textLines
                .flatMapValues(StreamProcessor::split)
                .groupBy((key, word) -> word)
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"));
       /// wordCounts.toStream().to("WordsWithCountsTopic", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        try {
            Thread.sleep(10000000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/


        IStreamFactory<String, String> factory= IStreamFactoryBuilder.PROVIDER.get().defineStream("word-stat").consumingTopic("test").
                usingUrlPort("localhost",9092).usingKeyValueSerdes(Serdes.String(),Serdes.String()).create();
        KTable<String,StringStat> table=factory.mapReduceRedirect(StreamProcessor::transformValue,StreamProcessor::mapKeyVal,StreamProcessor::reducer, Serdes.String(), new CombinedSerde<String,Long,StringStat>(),null);
        factory.run();
        Scanner scanner = new Scanner(System.in);
        String in="";
        while(!"stop".equals(scanner.nextLine()))
        {

        }
        factory.close();

    }

    private static List<String> split(String s) {
        return Arrays.asList(s.toLowerCase().split("\\W+"));
    }


    static Iterable<StringStat> transformValue(String s)
    {
        System.out.println("transform "+s);
        List<StringStat> stat= new ArrayList<>();
        String[] split=s.split("\\ ");
        Arrays.stream(split).forEach(str->{stat.add(new StringStat(str,1L));});
        return stat;

    }
    static String mapKeyVal(String s, StringStat stat)
    {
        return stat.key();
    }
    static StringStat reducer(StringStat acceptor, StringStat accepted)
    {
        return (StringStat) acceptor.merge(accepted);
    }

}
