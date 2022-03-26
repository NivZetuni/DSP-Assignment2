package com.sample.mr;

import java.io.IOException;

import java.util.*;
import java.util.regex.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Job2 {
    private Job job;
    private String inputPath;
    private String outputPath;

    public Job2(String inputPath, String outputPath) throws IOException {
        Configuration conf = new Configuration();
        conf.set("c0", "0");
        this.job = Job.getInstance(conf, "Job2");
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        init();
    }
    private void init() throws IOException{
        job.setJarByClass(Job2.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
    }

    public void runAndWait() throws IOException, InterruptedException, ClassNotFoundException {
        if (!job.waitForCompletion(true))
            System.exit(1);
    }


    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {


        @Override
        public void map(LongWritable lineNum, Text line, Context context) throws IOException,  InterruptedException {
            String[] keyValue = line.toString().split("\t");
            Text key = new Text(keyValue[0]);
            Text value = new Text(keyValue[1]);
            String[] words = keyValue[0].split(" ");
            if (words.length == 1 || words.length == 2)
                context.write(key, value);
            else if(words.length == 3){
                String word1 = words[0];
                String word2 = words[1];
                String word3 = words[2];

                Text newKey = new Text(word1 + " " + word2);

                String valStr = word3 + " " + value.toString();
                Text val = new Text(valStr);
                context.write(newKey, val);
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text,Text> {

        @Override
        public int getPartition(Text word, Text count, int numReducers) {
            return numReducers-1;
        }

    }

    public static class ReducerClass extends Reducer<Text,Text,Text, Text> {
        private long totalWords;

        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
            totalWords = 0;
        }


        @Override
        public void reduce(Text key, Iterable<Text> values, Reducer<Text,Text,Text, Text>.Context context) throws IOException, InterruptedException {
            if (key.toString().equals("*")){
                for(Text value: values) {
                    totalWords += Long.parseLong(value.toString());
                }
                return;
            }
            int c1 = 1;     // w2 occurrences
            int c2 = 1;     // w1*w2 occurrences

            LinkedList<String> valsStr = new LinkedList<String>();
            for(Text value: values) {
                String valStr = value.toString();
                String[] valsList = valStr.split(" ");
                if(valsList.length == 2){
                    c1 = Integer.parseInt(valsList[0]);
                    c2 = Integer.parseInt(valsList[1]);
                }
                else{
                    valsStr.add(valStr);
                }
            }
            String word3;
            int n1;     // w3 occurrences
            int n2;     // w2*w3 occurrences
            int n3;     // w1*w2*w3 occurrences
            for (String val: valsStr){
                String[] valList = val.split(" ");
                word3 = valList[0];
                n1 = Integer.parseInt(valList[1]);
                n2 = Integer.parseInt(valList[2]);
                n3 = Integer.parseInt(valList[3]);

                Text probability = calculateP(n1, n2, n3, totalWords, c1, c2);
                Text newKey = new Text(key.toString()+ " "+ word3);
                context.write(newKey, probability);
            }
        }

        @Override
        public void cleanup(Context context){

        }

        private Text calculateP(double n1, double n2, double n3, double c0, double c1, double c2){
            n1 = n1 == 0? 1 : n1;
            n2 = n2 == 0? 1 : n2;
            n3 = n3 == 0? 1 : n3;

            c0 = c0 == 0? 1 : c0;
            c1 = c1 == 0? 1 : c1;
            c2 = c2 == 0? 1 : c2;

            double k2 = (Math.log10(n2 + 1) +1) / (Math.log10(n2 + 1) +2);
            double k3 = (Math.log10(n3 + 1) +1) / (Math.log10(n3 + 1) +2);

            double p1 = (k3* (n3/c2));
            double p2 = ((1-k3) * k2 * (n2 / c1));
            double p3 = ((1 - k3) * (1- k2) * (n1 / c0));

            double p = p1 + p2 + p3;
            return new Text(String.valueOf(p));
        }
    }
    public static void main(String[] args) throws Exception {

        System.out.println("Starting Job 2.");
        Job2 job = new Job2(args[1], args[2]);
        job.runAndWait();
        System.out.println("Done job 2.");
    }
}