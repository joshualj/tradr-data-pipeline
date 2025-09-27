package com.tradr_data_pipeline;

import com.tradr_data_pipeline.processor.ForwardFilledCleanupService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

@SpringBootApplication
public class ForwardFilledDataCleaner {
    public static void main(String[] args) {
        ApplicationContext context = SpringApplication.run(ForwardFilledDataCleaner.class, args);
        ForwardFilledCleanupService service = context.getBean(ForwardFilledCleanupService.class);
        service.cleanUpData();
    }
}