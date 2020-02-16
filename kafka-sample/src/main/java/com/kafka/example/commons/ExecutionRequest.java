package com.kafka.example.commons;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class ExecutionRequest {
    public Method getMethod() {
        return method;
    }

    public Serializable[] getArgs() {
        return args;
    }

    public String getClazz() {
        return clazz;
    }

    Method method;
    Serializable[] args;
    String clazz;
    public ExecutionRequest()
    {

    }
    public ExecutionRequest(Method m,Serializable []args)
    {
        method=m;
        clazz=m.getDeclaringClass().getName();
        this.args= args;

    }
    public Object execute(Object o)
    {
        try {
            return method.invoke(o,args);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
        return null;

    }
}
