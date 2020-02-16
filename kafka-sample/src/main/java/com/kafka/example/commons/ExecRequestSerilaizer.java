package com.kafka.example.commons;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.*;
import java.lang.reflect.Method;
import java.rmi.UnexpectedException;
import java.util.HashMap;
import java.util.Map;

public class ExecRequestSerilaizer implements Deserializer<ExecutionRequest>,Serializer<ExecutionRequest> {

    static Map<Class,Map<String,Method>> mapMethods = new HashMap<Class, Map<String, Method>>();
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, ExecutionRequest executionRequest) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try
        {
            ObjectOutputStream oos= new ObjectOutputStream(bos);
            oos.writeUTF(executionRequest.getClazz());
            oos.writeUTF(executionRequest.getMethod().toString());
            oos.writeObject(executionRequest.getArgs());
            oos.close();
            return bos.toByteArray();

        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    Map<String,Method> classMethods(Class c)
    {
        Map<String,Method> map=null;
        synchronized (mapMethods)
        {
           map = mapMethods.computeIfAbsent(c,(key)-> {return new HashMap<String, Method>();});

        }
        synchronized (map)
        {
            if(map.isEmpty())
            {
                for(Method m:c.getDeclaredMethods())
                {
                    m.setAccessible(true);
                    map.put(m.toString(),m);
                }
            }
        }
        return map;
    }
    public ExecRequestSerilaizer()
    {

    }

    public ExecutionRequest deserialize(String s, byte[] bytes) {
        ByteArrayInputStream bis= new ByteArrayInputStream(bytes);
        try {
            ObjectInputStream ois= new ObjectInputStream(bis);
            String clazz= ois.readUTF();
            Class c= Class.forName(clazz);
            String strMethod= ois.readUTF();
            Serializable[] args= (Serializable[]) ois.readObject();
            Method m= classMethods(c).get(strMethod);
            if(m==null)
                throw new UnexpectedException(strMethod);
            return new ExecutionRequest(m,args);

        } catch (Exception e) {
            e.printStackTrace();
            //throw new RuntimeException(e);
        }
        return null;

    }

    public void close() {

    }
}
