package com.sample.mr;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import org.apache.log4j.BasicConfigurator;

public class Main {

    public static void main(String[] args){
        String bucket = "s3://dsp221ass2nivandgil";
        String key = "testtt";
        String aggregation = "true";

        BasicConfigurator.configure();
        AWSCredentialsProvider credentials = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder
                .standard()
                .withRegion("us-east-1")
                .withCredentials(credentials)
                .build();


        HadoopJarStepConfig hadoopJarStep1 = new HadoopJarStepConfig()
                .withJar(bucket + "/Job1.jar")
                .withMainClass("Job1")
                .withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data", bucket+"/output1/", aggregation);

        StepConfig stepConfig1 = new StepConfig()
                .withName("Job1")
                .withHadoopJarStep(hadoopJarStep1)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig hadoopJarStep2 = new HadoopJarStepConfig()
                .withJar(bucket + "/Job2.jar")
                .withMainClass("Job2")
                .withArgs(bucket+"/output1/" , bucket+"/output2/");

        StepConfig stepConfig2 = new StepConfig()
                .withName("Job2")
                .withHadoopJarStep(hadoopJarStep2)
                .withActionOnFailure("TERMINATE_JOB_FLOW");


        HadoopJarStepConfig hadoopJarStep3 = new HadoopJarStepConfig()
                .withJar(bucket + "/Job3.jar")
                .withMainClass("Job3")
                .withArgs(bucket+"/output2/", bucket+"/output3/");

        StepConfig stepConfig3 = new StepConfig()
                .withName("Job3")
                .withHadoopJarStep(hadoopJarStep3)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        //** set instances

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(5)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("2.6.0")
                .withEc2KeyName(key)
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        //** run the job flow

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("Flow")
                .withInstances(instances)
                .withSteps(stepConfig1, stepConfig2, stepConfig3)
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-5.11.0")
                .withLogUri(bucket + "/Logs/");


        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }
}