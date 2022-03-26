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


public class Job3 {
    private Job job;
    private String inputPath;
    private String outputPath;

    public Job3(String inputPath, String outputPath) throws IOException {
        Configuration conf = new Configuration();
        this.job = Job.getInstance(conf, "Job3");
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        init();
    }
    private void init() throws IOException{
        job.setJarByClass(Job3.class);
        job.setMapperClass(MapperClass.class);
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
            String[] lineValue = line.toString().split("\t");
            Text key = new Text(lineValue[0]);
            Double value = new Double(1- Double.valueOf(lineValue[1]));
            Text valueKey = new Text(value.toString());

            String[] keyValue = key.toString().split(" ");


            Text newKey = new Text(keyValue[0] + " " + keyValue[1] + " " + valueKey + " " + keyValue[2]);


            context.write(newKey, new Text(lineValue[1]));

        }
    }

    public static class ReducerClass extends Reducer<Text,Text,Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Reducer<Text,Text,Text, Text>.Context context) throws IOException, InterruptedException {

            String[] keyValue = key.toString().split(" ");
            Text newKey = new Text(keyValue[0]+ " " + keyValue[1]+ " " + keyValue[3]);
            for (Text value: values) {
                context.write(newKey,value);
            }

        }
    }
    public static void main(String[] args) throws Exception {

        System.out.println("Starting Job 3.");
        Job3 job = new Job3(args[1], args[2]);
        job.runAndWait();
        System.out.println("Done job 3.");
    }
}