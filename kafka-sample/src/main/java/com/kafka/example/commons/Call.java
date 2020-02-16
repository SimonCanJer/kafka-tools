package com.kafka.example.commons;

public class Call implements ICall {
    ThreadLocal prefix= new ThreadLocal();

    public Call()
    {

    }
    public void setPrefix(String s)
    {
        prefix.set(s);
    }

    @Override
    public String call(String s) {

        return String.valueOf(prefix.get())+"."+s;
    }
}
