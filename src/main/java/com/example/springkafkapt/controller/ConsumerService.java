package com.example.springkafkapt.controller;

import com.example.springkafkapt.config.ConsumerConfigCustom;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Profile("consumer")
@Service
public class ConsumerService {

    private final ConsumerConfigCustom consumerConfigCustom;

    private final Map<String, KafkaMessageListenerContainer<String, String>> consumerContainers = new ConcurrentHashMap<>();

    private final Map<String, List<String>> messageMap = new ConcurrentHashMap<>();

    public ConsumerService(ConsumerConfigCustom consumerConfigCustom) {
        this.consumerConfigCustom = consumerConfigCustom;
    }

    public void createConsumer(String consumerName, String topic, String groupId) {
        String clientId = UUID.randomUUID().toString();
        messageMap.putIfAbsent(consumerName, new ArrayList<>());

        ConsumerFactory<String, String> consumerFactory = consumerConfigCustom.createConsumerFactory(groupId, clientId);
        ContainerProperties containerProps = getContainerProperties(consumerName, topic);

        KafkaMessageListenerContainer<String, String> container = new KafkaMessageListenerContainer<>(consumerFactory, containerProps);
        container.start();
        consumerContainers.put(consumerName, container);
    }

    private ContainerProperties getContainerProperties(String consumerName, String topic) {
        ContainerProperties containerProps = new ContainerProperties(topic);

        containerProps.setMessageListener(new MessageListener<String, String>() {
            @Override
            public void onMessage(ConsumerRecord<String, String> record) {
                String message = "들어온 메시지 확인 >>  컨슈머 이름 : " + consumerName + ", 토픽 : " + record.topic() + ", 메시지 : " + record.value();
                System.out.println(message);
                messageMap.get(consumerName).add(record.value());
            }
        });

        return containerProps;
    }

    public List<String> getMessage(String consumerName) {
        return messageMap.getOrDefault(consumerName, new ArrayList<>());
    }

}
