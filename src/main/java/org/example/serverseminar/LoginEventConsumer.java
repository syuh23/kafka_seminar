package org.example.serverseminar;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class LoginEventConsumer {

    @KafkaListener(topics = "login-events", groupId = "test-seminar")
    public void consumeLoginEvent(String loginEvent) {
        System.out.println("Processing login event: " + loginEvent);
    }
}