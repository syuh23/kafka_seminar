package com.example.springkafkapt.controller;

import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@Profile("consumer")
@RequestMapping("/consumer")
public class ConsumerController {

    private final ConsumerService consumerService;

    public ConsumerController(ConsumerService consumerService) {
        this.consumerService = consumerService;
    }

    @PostMapping("/create")
    public String createConsumer(@RequestParam String consumerName, @RequestParam String topic, @RequestParam String groupId) {
        consumerService.createConsumer(consumerName, topic, groupId);
        return "Consumer created Successful";
    }

    @GetMapping("/message")  // -> 각 컨슈머 별로 다르게 출력되게끔
    public List<String> getMessage(@RequestParam String consumerName) {
        return consumerService.getMessage(consumerName);
    }

}