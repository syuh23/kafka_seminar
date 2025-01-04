package com.example.serverseminar;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;

@SpringBootApplication
public class ServerseminarApplication {

    public static void main(String[] args) {
        SpringApplication.run(ServerseminarApplication.class, args);

//        KafkaAdmin kafkaAdmin;
//        KafkaHeaders kafkaHeaders;
//        KafkaTemplate kafkaTemplate;
//        KafkaConfiguration
    }

}
