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

@Profile({"consumer", "consumer-two"})
@Service
public class ConsumerService {

    private final ConsumerConfigCustom consumerConfigCustom;

    private final Map<String, KafkaMessageListenerContainer<String, String>> consumerContainers = new ConcurrentHashMap<>();

    private final List<String> KafkaMessageList = new ArrayList<>();


    public ConsumerService(ConsumerConfigCustom consumerConfigCustom) {
        this.consumerConfigCustom = consumerConfigCustom;
    }

    public void createConsumer(String topic, String groupId) {

        String clientId = UUID.randomUUID().toString();
        String key = groupId + "_" + topic + "_" + clientId;

        ConsumerFactory<String, String> consumerFactory = consumerConfigCustom.createConsumerFactory(groupId, clientId);
        ContainerProperties containerProps = new ContainerProperties(topic);

        containerProps.setMessageListener(new MessageListener<String, String>() {
            @Override
            public void onMessage(ConsumerRecord<String, String> record) {
                String message = "들어온 메시지 확인 >>>  토픽 : " + record.topic() + ", 메시지 : " + record.value();
                System.out.println(message);
                KafkaMessageList.add(message);
            }
        });

        KafkaMessageListenerContainer<String, String> container =
                new KafkaMessageListenerContainer<>(consumerFactory, containerProps);
        container.start();
        consumerContainers.put(key, container);
    }


    public List<String> getAllMessages() {
        return new ArrayList<>(KafkaMessageList);
    }

}
