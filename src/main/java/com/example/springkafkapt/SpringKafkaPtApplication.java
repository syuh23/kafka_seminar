package com.example.springkafkapt;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;

@SpringBootApplication(exclude = {KafkaAutoConfiguration.class})  // KafkaAutoConfiguration의 실행을 막음
public class SpringKafkaPtApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringKafkaPtApplication.class, args);
    }
}
