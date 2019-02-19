package com.gonnect.dataengineering.springbatch.steps;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.aggregate.AggregateApplication;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.concurrent.TimeUnit;

import static org.springframework.http.HttpHeaders.ACCEPT;
import static org.springframework.http.HttpHeaders.CONTENT_TYPE;
import static org.springframework.http.MediaType.TEXT_PLAIN_VALUE;

@Slf4j
public class RawToCleanseTasklet implements Tasklet {


    private final AmazonS3 amazonS3;
    private final Processor processor;
    private final MessageCollector messageCollector;

    public RawToCleanseTasklet(AmazonS3 amazonS3, Processor processor, MessageCollector messageCollector) {
        this.amazonS3 = amazonS3;
        this.processor = processor;
        this.messageCollector = messageCollector;
    }


    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

        System.out.println("Raw To Cleanse");


        processor.input().send(MessageBuilder.withPayload("Hello").setHeader(CONTENT_TYPE, TEXT_PLAIN_VALUE).setHeader(ACCEPT,TEXT_PLAIN_VALUE).build());

        Message<?> receivedMessage = messageCollector.forChannel(processor.output()).poll(1, TimeUnit.SECONDS);

        System.out.println(receivedMessage.getPayload());




        return RepeatStatus.FINISHED;
    }
}
