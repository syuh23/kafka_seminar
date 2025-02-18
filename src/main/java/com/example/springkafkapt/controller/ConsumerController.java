package com.example.springkafkapt.controller;

import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@Profile({"consumer", "consumer-two"})
@RequestMapping("/consumer")
public class ConsumerController {

    private final ConsumerService consumerService;

    public ConsumerController(ConsumerService consumerService) {
        this.consumerService = consumerService;
    }

    @PostMapping("/create")
    public String createConsumer(@RequestParam String topic, @RequestParam String groupId) {
        consumerService.createConsumer(topic, groupId);
        return "Consumer created Successful";
    }

    @GetMapping("/messages")
    public List<String> getAllMessages() {
        return consumerService.getAllMessages();
    }

}