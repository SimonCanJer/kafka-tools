package com.stream.kafka.api;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.*;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

public class CombinedSerde<Key extends Serializable,Value extends Serializable,C extends Combined<Key,Value>> implements Serde<C> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }
    static Map<String,Constructor<? extends Combined>> smCreators = new HashMap<>();

    @Override
    public void close() {

    }
   static Constructor<? extends Combined> getCtor(String name)
   {
       synchronized (smCreators)
       {

           return smCreators.computeIfAbsent(name,(k)->{return ctorFromCName(name);});
       }

   }

    private static Constructor<? extends Combined> ctorFromCName(String name) {
        try {
            Constructor<? extends Combined> ctor=null;
            Class c=Class.forName(name);
            try {
                ctor=(Constructor<? extends Combined>) c.getDeclaredConstructor(new Class[]{});
            }
            catch(Exception e)
            {
                Constructor[] ctors=c.getDeclaredConstructors();
                for(Constructor cctor:ctors)
                {
                    if(cctor.getParameterCount()==2)
                    {
                        Class[] params= cctor.getParameterTypes();
                        if(Serializable.class.isAssignableFrom(params[1]) && Serializable.class.isAssignableFrom(params[1]))
                        {
                            ctor=cctor;
                            break;
                        }
                    }
                }

            }
            ctor.setAccessible(true);
            return ctor;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public Serializer<C> serializer() {
        return new Serializer<C>() {
            @Override
            public void configure(Map<String, ?> map, boolean b) {

            }

            @Override
            public byte[] serialize(String s, Combined combined) {
                ByteArrayOutputStream stream= new ByteArrayOutputStream();
                try {
                    ObjectOutputStream oos= new ObjectOutputStream(stream);
                    oos.writeUTF(combined.getClass().getName());
                    oos.writeObject(combined.key());
                    oos.writeObject(combined.measure());
                    oos.close();
                    return stream.toByteArray();
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

            }

            @Override
            public void close() {

            }
        };
    }



    @Override
    public Deserializer<C> deserializer() {
        return new Deserializer<C>() {
            @Override
            public void configure(Map<String, ?> map, boolean b) {

            }

            @Override
            public C deserialize(String s, byte[] bytes) {
                ByteArrayInputStream bis= new ByteArrayInputStream(bytes);
                try {
                    ObjectInputStream ois = new ObjectInputStream(bis);
                    String className= ois.readUTF();
                    Key k= (Key) ois.readObject();
                    Value m= (Value) ois.readObject();
                    ois.close();
                    Constructor<C> ctor= (Constructor<C>) getCtor(className);
                    if(ctor==null)
                        return null;

                    if(ctor.getParameterCount()==0) {
                        C cmb= ctor.newInstance();
                        cmb.mKey = k;
                        cmb.mMeasure = m;
                        return cmb;

                    }
                    return ctor.newInstance(k,m);

                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                }

            }

            @Override
            public void close() {

            }
        };
    }
}
