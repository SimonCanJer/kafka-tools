package com.stream.kafka.api;

import java.io.Serializable;

public abstract class Combined<Key extends Serializable,Measure extends Serializable> {
    Key mKey;
    Measure mMeasure;
    protected Combined(Key key,Measure m)
    {
        mKey=key;
        mMeasure = m;
    }
    Combined()
    {

    }
    public Combined merge(Combined<Key,Measure> other)
    {
        if(!mKey.equals(other.key()))
        {
            return this;
        }
        mMeasure= combine(other.mMeasure);
        return this;
    }

    protected abstract Measure combine(Measure mMeasure);
    public Measure measure()
    {
        return mMeasure;
    }
    public void key(Key key)
    {
        mKey=key;
    }
    public void measure(Measure m)
    {
       mMeasure =m;
    }
    public Key key()
    {
        return mKey;
    }

}
