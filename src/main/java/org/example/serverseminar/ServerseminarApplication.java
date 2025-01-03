package org.example.serverseminar;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ServerseminarApplication {

    public static void main(String[] args) {
        SpringApplication.run(ServerseminarApplication.class, args);
        KafkaProducer kafkaProducer;
        // 여기서 코드 쳐보면서 한번 시작점을 찾아보려고 했음

    }

}
