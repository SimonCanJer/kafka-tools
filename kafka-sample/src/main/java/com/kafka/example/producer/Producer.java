package com.kafka.example.producer;

import com.kafka.example.commons.ExecRequestSerilaizer;
import com.kafka.example.commons.ExecutionRequest;
import com.kafka.example.commons.ICall;
import com.messaging.kafka.producer.api.IProducer;
import com.messaging.kafka.producer.api.IProducerFactory;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Producer {
    static class ProxyClass implements InvocationHandler
    {
        ICall call;
        IProducer<String,ExecutionRequest> producer;
        String key= "def";
        void setKey(String s)
        {
            key=s;
        }
        ProxyClass()
        {
            call= (ICall) Proxy.newProxyInstance(this.getClass().getClassLoader(),new Class[]{ICall.class}, this);
        }
        void init()
        {
            producer=IProducerFactory.factory.get().createWithId("exampleSrc");
            producer.withBroker("localhost", 9092).withSerializers(StringSerializer.class,ExecRequestSerilaizer.class).init();

        }
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            Serializable[] serArgs= new Serializable[args.length];
            for(int i=0;i<args.length;i++)
                serArgs[i]= (Serializable) args[i];
            producer.put("myQ", 1, key, new ExecutionRequest(method, serArgs), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                }
            }).get(1000,TimeUnit.MILLISECONDS);
            return null;
        }
    }
    // Complete the anagram function below.

    static public void main(String[] args)
    {

         ProxyClass proxy = new ProxyClass();
        proxy.setKey("1");
        proxy.init();
        Scanner sc= new Scanner(System.in);
        while(true)
        {
            String s= sc.nextLine();
            if(s.length()==0|| s.equals("_end_"))
            {
                break;
            }
            proxy.call.call(s);
        }


    }

}
