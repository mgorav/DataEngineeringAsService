package com.gonnect.dataengineering.springbatch.steps;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

@Slf4j
public class LandingToRawTasklet implements Tasklet {


    private final AmazonS3 amazonS3;

    public LandingToRawTasklet(AmazonS3 amazonS3) {
        this.amazonS3 = amazonS3;
    }


    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

        String rawBucketName = (String)chunkContext.getStepContext().getJobParameters().get("rawBucketName");
        String landingBucketName = (String)chunkContext.getStepContext().getJobParameters().get("landingBucketName");

        //create raw bucket, if it does not exist
        if(!amazonS3.doesBucketExistV2(rawBucketName)) {
            log.info("Bucket name does not exist");
            throw new RuntimeException("No bucket exists");
        }


        //copy landing bucket contents to raw bucket
        for (S3ObjectSummary objectSummary:amazonS3.listObjectsV2(landingBucketName).getObjectSummaries()) {
            amazonS3.copyObject(new CopyObjectRequest(objectSummary.getBucketName(), objectSummary.getKey(), rawBucketName, objectSummary.getKey()));
            amazonS3.deleteObject(objectSummary.getBucketName(), objectSummary.getKey());
        }

        return RepeatStatus.FINISHED;
    }
}
