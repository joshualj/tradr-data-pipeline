package com.tradr_data_pipeline;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

//@SpringBootApplication
public class StockDataProcessorApp {
    public static void main(String[] args) {
        SpringApplication.run(StockDataProcessorApp.class, args);
    }
}

//    public static void main(String[] args) {
//        ApplicationContext context = SpringApplication.run(com.tradr_data_pipeline.StockDataProcessorApp.class, args);
//        StockDataProcessorService service = context.getBean(StockDataProcessorService.class);
//        service.run(args);
//    }