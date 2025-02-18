package com.example.springkafkapt.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Configuration
@Profile({"consumer", "consumer-two"})
public class ConsumerConfigCustom {

    private final KafkaProperties properties;

    public ConsumerConfigCustom(KafkaProperties properties) {
        this.properties = properties;
    }

    public ConsumerFactory<String, String> createConsumerFactory(String groupId, String clientId) {
        Map<String, Object> props = this.properties.buildConsumerProperties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        return new DefaultKafkaConsumerFactory<>(props);
    }

}