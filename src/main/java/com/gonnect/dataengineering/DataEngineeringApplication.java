package com.gonnect.dataengineering;

import com.amazonaws.services.s3.AmazonS3;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.app.python.http.processor.PythonHttpProcessorConfiguration;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@Import(PythonHttpProcessorConfiguration.class)
public class DataEngineeringApplication implements CommandLineRunner {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job dataEngineeringJob;

    @Autowired
    private AmazonS3 amazonS3;


    public static void main(String[] args) {

        SpringApplication.run(DataEngineeringApplication.class, args);

    }

    @Override
    public void run(String... args) throws Exception {

        amazonS3.createBucket("rawbucket");
        amazonS3.createBucket("landingbucket");
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("rawBucketName", "rawbucket")
                .addString("landingBucketName", "landingbucket")
                .toJobParameters();

        jobLauncher.run(dataEngineeringJob, jobParameters);
    }
}
