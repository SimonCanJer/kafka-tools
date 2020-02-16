package com.messaging.kafka.concrete;

import com.messaging.kafka.consumer.api.IConsumingHub;
import com.messaging.kafka.consumer.api.IKafkaConsumerPoll;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class KafkaConsumerPoll<K,V> implements IKafkaConsumerPoll<K,V> {
    Set<String> mQueues = new HashSet<>();
    Properties mProperties= new Properties();
    StringBuilder mSBuilder = new StringBuilder();
    String mStrGroup=UUID.randomUUID().toString();
    boolean started=false;
    int numConsumers=1;
    ExecutorService exec;
    AtomicInteger mStartedRuns= new AtomicInteger(0);
    public KafkaConsumerPoll()
    {

        mProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        mProperties.put(ConsumerConfig.GROUP_ID_CONFIG, mStrGroup);
    }
    @Override
    public IKafkaConsumerPoll basingOnQueues(String[] queues) {
        mQueues.addAll(Arrays.asList(queues));
        return this;
    }

    @Override
    public IKafkaConsumerPoll addUrlPort(String url, int port) {
        if(mSBuilder.length()>0)
            mSBuilder.append(',');
        mSBuilder.append(url).append(':').append(port);
        return this;
    }

    @Override
    public IKafkaConsumerPoll withGroupName(String groupName) {
         mProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupName);
         return this;
    }

    @Override
    public IKafkaConsumerPoll usingSerializers(Class<? extends Deserializer<K>> serK, Class<? extends Deserializer<V>> serV) {
        mProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,serV.getName());
        mProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,serK.getName());
        return this;
    }

    @Override
    public IKafkaConsumerPoll involvingPercentOfProcessors(float f) {

       if(f>1)
       {
           throw new RuntimeException("wrong percentage of cores");
       }
       numConsumers= (int) (Runtime.getRuntime().availableProcessors()*f);
       return this;
    }
    List<ConsumerRun> runs= new ArrayList<>();
    class ConsumerRun implements Runnable {
        private KafkaConsumer<K,V> mConsumer;
        Consumer<ConsumerRecord<K, V>> processor;
        ConsumerRun(Consumer<ConsumerRecord<K,V>> processor)
        {
            this.processor=processor;
        }
        void shutdown()
        {
            try {
                mConsumer.close();
            }
            catch(Exception e)
            {

            }
        }

        @Override
        public void run() {
            mConsumer= new KafkaConsumer<K, V>(mProperties);
            mConsumer.subscribe(mQueues);
            mStartedRuns.incrementAndGet();
            while (true) {
                ConsumerRecords<K, V> records = mConsumer.poll(10000);

                if (records == null)
                    continue;
                Set<TopicPartition> partitions=records.partitions();
                for(TopicPartition tp :partitions)
                {
                    records.records(tp).stream().forEach(r-> accept(r));

                }



            }

        }

        private void accept(ConsumerRecord<K,V> r) {
            try {
                processor.accept(r);
            }
            catch(Exception e)
            {

            }
        }
    }
    @Override
    public  void runWithConsumer( Consumer<ConsumerRecord<K, V>> processor) {

        if(!started)
        {
            synchronized (this)
            {
                if(!started)
                {

                    exec= Executors.newFixedThreadPool(numConsumers);
                    for(int i=0;i<numConsumers;i++) {
                        ConsumerRun cr;
                        exec.submit(cr=new ConsumerRun(processor));
                        runs.add(cr);
                    }
                }

            }
            installShutdownHook();

        }

    }

    @Override
    public void run() {
        runWithConsumer(hubConsumer.getListener());
    }

    class ConsumingHub implements IConsumingHub<K,V>
    {
        Map<String,BiConsumer<K,V>> map= new HashMap<>();
        String index(String topic, int partition)
        {
            return topic+"/"+partition;
        }


        @Override
        public void listen(String topic, int partition, BiConsumer<K, V> listener) {
            map.put(index(topic,partition),listener);

        }


        public Consumer<ConsumerRecord<K, V>> getListener() {
            return new Consumer<ConsumerRecord<K, V>>()
            {

                @Override
                public void accept(ConsumerRecord<K, V> kvConsumerRecord) {
                    String index=index(kvConsumerRecord.topic(),kvConsumerRecord.partition());
                    BiConsumer<K,V> bi=map.get(index);
                    if(bi!=null)
                    {
                        bi.accept(kvConsumerRecord.key(),kvConsumerRecord.value());
                    }
                }
            };
        }
    }

    ConsumingHub hubConsumer = new ConsumingHub();


    @Override
    public IConsumingHub<K, V> getConsumingHub() {
        return hubConsumer;
    }

    void installShutdownHook()
    {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
               shutdown();
            }
        });
    }

    @Override
    public void shutdown() {
        runs.stream().forEach(r->r.shutdown());
        exec.shutdown();

    }
}
