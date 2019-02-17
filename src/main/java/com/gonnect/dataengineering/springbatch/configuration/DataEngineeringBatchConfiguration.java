package com.gonnect.dataengineering.springbatch.configuration;

import com.amazonaws.services.s3.AmazonS3;
import com.gonnect.dataengineering.springbatch.steps.LandingToRawTasklet;
import com.gonnect.dataengineering.springbatch.steps.RawToCleanseTasklet;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableBatchProcessing
public class DataEngineeringBatchConfiguration {

    @Autowired
    private JobBuilderFactory jobs;

    @Autowired
    private StepBuilderFactory steps;

    @Autowired
    private AmazonS3 amazonS3;

    @Bean
    public Step landingToRawTasklet()  {
        return steps.get("landingToRawTasklet").tasklet(new LandingToRawTasklet(amazonS3)).build();
    }

    @Bean
    public Step rawToCleanseTasklet()  {
        return steps.get("rawToCleanseTasklet").tasklet(new RawToCleanseTasklet(amazonS3)).build();
    }

    @Bean
    public Job dataEngineeringJob() {
        return jobs.get("dataEngineeringJob").incrementer(new RunIdIncrementer()).start(landingToRawTasklet()).next(rawToCleanseTasklet()).build();
    }

}

