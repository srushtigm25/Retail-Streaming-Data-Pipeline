package com.lakehouse.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;

@SpringBootApplication
@EnableCaching
public class LakehouseApiApplication {
    public static void main(String[] args) {
        SpringApplication.run(LakehouseApiApplication.class, args);
    }
}
