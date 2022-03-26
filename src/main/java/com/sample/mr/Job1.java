package com.sample.mr;

import java.io.IOException;

import java.util.*;
import java.util.regex.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Job1 {
    private Job job;
    private String inputPath;
    private String outputPath;
    private boolean localAgg;

    public Job1(String inputPath, String outputPath, String localAgg) throws IOException {
        Configuration conf = new Configuration();
        this.job = Job.getInstance(conf, "Job1");
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.localAgg = false;
        if(localAgg.equals("true")){
            this.localAgg=true;
        }
        init();
    }
    private void init() throws IOException{
        job.setJarByClass(Job1.class);
        job.setMapperClass(Job1.MapperClass.class);
        if (localAgg){
            job.setCombinerClass(Job1.CombinerClass.class);
        }
        job.setReducerClass(Job1.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Value.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
    }

    public void runAndWait() throws IOException, InterruptedException, ClassNotFoundException {
        if (!job.waitForCompletion(true))
            System.exit(1);
    }

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Value> {

        public void map(LongWritable key, Text line, Context context
        ) throws IOException, InterruptedException {
            // get words
            String[] splitValues = line.toString().split("\\s+");
            String word1 = splitValues[0];
            String word2 = splitValues[1];
            String word3 = splitValues[2];
            //get occurrences
            int occ = Integer.parseInt(splitValues[4]);

            if (!checkIfHebrew(word1, word2, word3)){
                return;
            }

            Text key1 = new Text(word1);
            Text key2 = new Text(word2);
            Text key3 = new Text(word3);

            Value val1 = new Value(occ);
            Value val2 = new Value(occ,key1);
            Value val3 = new Value(occ,key1, key2);

            context.write(key1, val1);
            context.write(key2, val2);
            context.write(key3, val3);

            Text star = new Text("*");
            context.write(star, val1);
        }

        private boolean checkIfHebrew(String word1, String word2, String word3) {
            Pattern hebrewPattern = Pattern.compile("[א-ת]+[א-ת]");
            return hebrewPattern.matcher(word1).matches() && hebrewPattern.matcher(word2).matches() &&hebrewPattern.matcher(word3).matches();
        }
    }

    public static class CombinerClass extends Reducer<Text,Value,Text,Value> {


        @Override
        public void reduce(Text key, Iterable<Value> values, Context context) throws IOException,  InterruptedException {
            // initialize reducer values.
            LongWritable sum = new LongWritable(0);
            Stripe newPairMap = new Stripe();
            Stripe newTrioMap = new Stripe();

            //update the values.
            for (Value value : values) {
                sum.set(sum.get()+value.getOccurrences());
                newPairMap.add(value.getPairMap());
                newTrioMap.add(value.getTrioMap());
            }

            Value newVal = new Value(sum.get(), newPairMap, newTrioMap);

            context.write(key,newVal);

        }
    }


    public static class ReducerClass extends Reducer<Text,Value,Text, Text> {


        public void reduce(Text key, Iterable<Value> values, Context context) throws IOException, InterruptedException {
            // initialize reducer values.
            LongWritable sum = new LongWritable(0);
            Stripe newPairMap = new Stripe();
            Stripe newTrioMap = new Stripe();

            //update the values.
            for (Value value : values) {
                sum.set(sum.get()+value.getOccurrences());
                newPairMap.add(value.getPairMap());
                newTrioMap.add(value.getTrioMap());
            }
            Text newSum = new Text(sum.toString());

            if(key.toString().equals("*")){
                context.write(key, newSum);
                return;
            }

            for (Writable pairKey: newPairMap.keySet()){
                long val = newPairMap.getOccurrences(newPairMap, pairKey);
                context.write(appendText((Text)pairKey, key),appendText(newSum,new Text(String.valueOf(val))));
            }

            for (Writable trioKey: newTrioMap.keySet()){
                long valTrio = newTrioMap.getOccurrences(newTrioMap, trioKey);
                String[] trioKeyAsArray = trioKey.toString().split(" ");
                Text pairKey = new Text(trioKeyAsArray[1]);
                long valPair = newPairMap.get(pairKey)== null ? 0 : newPairMap.getOccurrences(newPairMap, pairKey);
                Text newKey = new Text(appendText((Text)trioKey, key));
                Text val = new Text(appendText(appendText(newSum, new Text(String.valueOf(valPair))),new Text(String.valueOf(valTrio))));
                context.write(newKey,val);
            }
        }


        private Text appendText(Text first, Text second){
            String pairWords = first.toString() + " " + second.toString();
            return new Text(pairWords);
        }



    }

    public static void main(String[] args) throws Exception {

        System.out.println("Starting Job 1.");
        Job1 job = new Job1(args[1], args[2], args[3]);
        job.runAndWait();
        System.out.println("Done job 1.");

    }
}
