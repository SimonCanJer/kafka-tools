package com.kafka.example.consumer;

import com.kafka.example.commons.Call;
import com.kafka.example.commons.ExecRequestSerilaizer;
import com.kafka.example.commons.ExecutionRequest;
import com.messaging.kafka.consumer.api.IConsumerBuilder;
import com.messaging.kafka.consumer.api.IKafkaConsumerPoll;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class Handler {



    static public void main(String[] args)
    {

        Call call= new Call();


        IConsumerBuilder builder=IConsumerBuilder.theBuilder.get();
        IKafkaConsumerPoll<String,ExecutionRequest> poll=builder.consumerPoll("call",
                StringDeserializer.class, ExecRequestSerilaizer.class).basingOnQueues(new String[]{"myQ"}).addUrlPort("localhost",9092);
        poll.runWithConsumer(new Consumer<ConsumerRecord<String,ExecutionRequest>>() {

            @Override
            public void accept(ConsumerRecord<String, ExecutionRequest> executionRequestConsumerRecord) {
                System.out.println("offset +"+executionRequestConsumerRecord.offset()+" part "+executionRequestConsumerRecord.partition()+" time "+executionRequestConsumerRecord.timestamp());
                call.setPrefix(executionRequestConsumerRecord.key());
                System.out.println(executionRequestConsumerRecord.value().execute(call));

            }
        });
    }

}
