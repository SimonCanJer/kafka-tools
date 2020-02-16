package com.messaging.kafka.admin;

import com.messaging.kafka.admin.api.IAdminSite;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.streams.kstream.KStream;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

public class KafkaAdminToolsTest {

    KafkaAdminTools tools = new KafkaAdminTools();
    IAdminSite site=null;
    @org.junit.Test
    public void test() {
        Throwable error= null;

        try {
            site = tools.withUrlAndPort("localhost", 9092).construct();
        }
        catch(Throwable t)
        {
            error=t;
        }
        assertNull(error);
        topicNames();

    }
    void createTopics()
    {
        KStream<String,String> k;
            KafkaFuture<Void> f= site.withTopicBatchBuilder().add("topic2332",1,(short)1).commit();
        Throwable th=null;
        try
        {
            f.get(10000, TimeUnit.MILLISECONDS);
        }
        catch(Throwable t)
        {
            th=t;
        }
        assertNull(th);
    }


    void topicNames()
    {
        Exception error=null;
        KafkaFuture<Set<String>> f=site.availableTopicNames();
        try {
            Set<String> res= f.get(5000, TimeUnit.MILLISECONDS);
            assertTrue(res.size()>0);
            for(String s: res)
            {
                System.out.println("q: "+s);
            }
        } catch (Exception e) {
            e.printStackTrace();
            error=e;
        }
        assertNull(error);


    }
}