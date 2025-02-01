package com.example.springkafkapt.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@Profile("producer")
public class ProducerController {
    private final KafkaTemplate<String, String> kafkaTemplate;

    @PostMapping("/send")
    public String sendMessage(@RequestParam String message) {

        kafkaTemplate.send("test-topic", message);
        System.out.println("Sent message: " + message);

        return "Sent message : " + message + "  Successfully";
    }
}