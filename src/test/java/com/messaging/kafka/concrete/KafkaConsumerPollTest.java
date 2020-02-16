package com.messaging.kafka.concrete;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

import java.util.function.Consumer;

import static org.junit.Assert.*;

public class KafkaConsumerPollTest {

    KafkaConsumerPoll<String,Long> poll= new KafkaConsumerPoll<>();

    @Test
    public void test()
    {
        basingOnQueues();
        addUrlPort();
        withGroupName();
        usingSerializers();
        involvingPercentOfProcessors();
        poll.runWithConsumer(new Consumer<ConsumerRecord<String, Long>>() {
            @Override
            public void accept(ConsumerRecord<String, Long> stringLongConsumerRecord) {

            }
        });
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        assertEquals(poll.mStartedRuns.get(),poll.numConsumers);
        poll.shutdown();
    }

    public void basingOnQueues() {
        poll.basingOnQueues(new String[]{"myQ"});
        assertTrue(poll.mQueues.contains("myQ"));
        assertEquals(poll.mQueues.size(),1);

    }


    public void addUrlPort() {
        poll.addUrlPort("localhost",9092);
        assertEquals(poll.mSBuilder.toString(),"localhost:9092");

    }


    public void withGroupName() {
        poll.withGroupName("myGroup");
        assertEquals(poll.mProperties.get(ConsumerConfig.GROUP_ID_CONFIG),"myGroup");

    }


    public void usingSerializers() {
        poll.usingSerializers(StringDeserializer.class,LongDeserializer.class);
        assertEquals(poll.mProperties.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG),StringDeserializer.class.getName());
        assertEquals(poll.mProperties.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG),LongDeserializer.class.getName());

    }


    public void involvingPercentOfProcessors() {
        int processors= (int) (( Runtime.getRuntime().availableProcessors())*0.5f);
        poll.involvingPercentOfProcessors(0.5f);
        assertEquals(processors,poll.numConsumers);
    }
}