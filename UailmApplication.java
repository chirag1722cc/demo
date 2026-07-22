package com.uailm;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class UailmApplication {
    public static void main(String[] args) {
        SpringApplication.run(UailmApplication.class, args);
    }
}
