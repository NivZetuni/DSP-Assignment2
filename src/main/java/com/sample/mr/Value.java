package com.sample.mr;

import org.apache.hadoop.io.*;

import java.io.*;

public class Value implements Writable{

    private LongWritable occ;
    private Stripe pair;
    private Stripe trio;

    public Value(){
        this.occ = new LongWritable(0);
        pair = new Stripe();
        trio = new Stripe();
    }

    public Value(int occ){
        this.occ = new LongWritable(occ);
        pair = new Stripe();
        trio = new Stripe();
    }
    public Value(int occ,Text firstWord){
        this.occ = new LongWritable(0);
        pair = new Stripe();
        pair.put(firstWord, new LongWritable(occ));
        trio = new Stripe();
    }
    public Value(int occ, Text firstWord, Text secondWord){
        this.occ = new LongWritable(0);
        pair = new Stripe();
        trio = new Stripe();
        Text pairWords = appendText(firstWord, secondWord);
        trio.put(pairWords, new LongWritable(occ));
    }

    public Value(long occ, Stripe pairMap, Stripe trioMap) {
        this.occ = new LongWritable(occ);
        pair = pairMap;
        trio = trioMap;
    }

    private Text appendText(Text first, Text second){
        String pairWords = first.toString() + " " + second.toString();
        return new Text(pairWords);
    }

    public long getOccurrences() {
        return occ.get();
    }

    public Stripe getPairMap() {
        return pair;
    }

    public Stripe getTrioMap() {
        return trio;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        occ.write(dataOutput);
        pair.write(dataOutput);
        trio.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        occ.readFields(dataInput);
        pair.readFields(dataInput);
        trio.readFields(dataInput);
    }
}
