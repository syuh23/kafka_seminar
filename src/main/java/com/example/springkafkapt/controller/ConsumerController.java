package com.example.springkafkapt.controller;

import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@RestController
@Profile({"consumer-one", "consumer-two"})
public class ConsumerController {
    private final List<String> messages = new ArrayList<>();

    @KafkaListener(topics = "test-topic", groupId = "kafka-test")
    public void listen(@Payload String message) {
        System.out.println("디버깅 확인용 메시지 : " + message);
        messages.add(message);
    }

    @GetMapping("/get")
    public List<String> getMessages() {
        System.out.println("Received messages: " + messages);
        return messages;
    }
}