package com.messaging.kafka;

import java.lang.reflect.Constructor;
import java.util.function.Supplier;

public abstract class InterfaceInstanceFactory<INTERFACE> {
     INTERFACE mBuilder=null;
     protected abstract String getTargetClass();

     private INTERFACE  get()
    {
        if(mBuilder==null)
        {

            synchronized (this)
            {
                if(mBuilder==null)
                {
                    String builder=getTargetClass();
                    try {
                        Constructor<INTERFACE> ctor=(Constructor<INTERFACE>)Class.forName(builder).getDeclaredConstructor();
                        ctor.setAccessible(true);

                        mBuilder = (INTERFACE)ctor. newInstance();
                    }
                    catch(Throwable err)
                    {
                        throw new RuntimeException(err);
                    }
                }
            }
        }
        return mBuilder;

    }
    public Supplier<INTERFACE> factory()
    {
        return this::get;
    }
}
