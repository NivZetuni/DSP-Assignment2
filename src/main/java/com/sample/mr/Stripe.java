package com.sample.mr;

import org.apache.hadoop.io.*;

public class Stripe extends MapWritable {

    public void add(Stripe from) {
        for (Writable fromKey : from.keySet()) {
            if (containsKey(fromKey)) {
                put(fromKey, new LongWritable(((LongWritable) get(fromKey)).get() + ((LongWritable) from.get(fromKey)).get()));
            } else {
                put(fromKey, from.get(fromKey));
            }
        }
    }

    // assume key is an existed key in from
    public long getOccurrences(Stripe from,Writable key){
        return ((LongWritable)from.get(key)).get();
    }
}
