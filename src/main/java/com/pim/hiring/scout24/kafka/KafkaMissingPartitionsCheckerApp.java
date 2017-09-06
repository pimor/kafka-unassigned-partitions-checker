package com.pim.hiring.scout24.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Created on 22/08/2017.
 */
@SpringBootApplication
@EnableScheduling
public class KafkaMissingPartitionsCheckerApp {

    public static void main(String[] args) {
        SpringApplication.run(KafkaMissingPartitionsCheckerApp.class, args);
    }
}
