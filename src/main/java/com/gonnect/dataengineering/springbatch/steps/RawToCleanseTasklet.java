package com.gonnect.dataengineering.springbatch.steps;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

@Slf4j
public class RawToCleanseTasklet implements Tasklet {


    private final AmazonS3 amazonS3;

    public RawToCleanseTasklet(AmazonS3 amazonS3) {
        this.amazonS3 = amazonS3;
    }


    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

        System.out.println("Raw To Cleanse");

        return RepeatStatus.FINISHED;
    }
}
