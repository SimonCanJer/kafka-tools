package com.kafka.example.stream;

import com.messaging.kafka.producer.api.IProducer;
import com.messaging.kafka.producer.api.IProducerFactory;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;

public class Producer {




    static public void main(String [] args)
    {

        IProducer<String,String> stringProducer = IProducerFactory.factory.get().createWithId("src").
                withBroker("localhost",9092).withSerializers(StringSerializer.class,StringSerializer.class);
        stringProducer.init();
        Scanner scanner = new Scanner(System.in);
        while(true)
        {
            String line= scanner.nextLine();
            if(line.length()==0||line.equals("stop!!!"))
            {
                break;

            }
            stringProducer.put("test", "terminal1", line, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    System.out.println("sent "+String.valueOf(e));
                }
            });
        }
    }
}
