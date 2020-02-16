package com.kafka.example.commons;

import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;
import java.lang.reflect.Method;

import static org.junit.Assert.*;

public class ExecRequestSerilaizerTest {
    static String recentlyCalled = "";
    static class Call
    {
        void call(String call)
        {
            recentlyCalled= call;

        }
    }
    byte[] serialization = null;
    ExecRequestSerilaizer serilaizer=  new ExecRequestSerilaizer();
    @Test
    public  void test()
    {
        serialize();
        deserialize();

    }
    public void serialize()  {
        Exception error=null;
        try {
            Method m = Call.class.getDeclaredMethod("call", new Class[]{String.class});
            ExecutionRequest req = new ExecutionRequest(m, new Serializable[]{"call"});
            serialization = serilaizer.serialize("key", req);
            Assert.assertNotNull(serialization);
        }
        catch(Exception e)
        {
            error=e;
        }
        Assert.assertNull(error);;


    }

    public void deserialize() {
        Throwable error=null;
        try
        {
            ExecutionRequest req= serilaizer.deserialize(null,serialization);
            req.execute(new Call());
            assertEquals(recentlyCalled,"Call");
        }
        catch(Throwable e)
        {
            error= e;

        }
    }

}