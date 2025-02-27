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

    // 각 동적 컨슈머(KafkaMessageListenerContainer 인스턴스)를 관리하기 위한 Map
    private final Map<String, KafkaMessageListenerContainer<String, String>> consumerContainers = new ConcurrentHashMap<>();

    private final List<String> kafkaMessageList = new ArrayList<>();


    public ConsumerService(ConsumerConfigCustom consumerConfigCustom) {
        this.consumerConfigCustom = consumerConfigCustom;
    }


    // kafka consumer 생성
    public void createConsumer(String topic, String groupId) {
        List<String> messageList = new ArrayList<>();
        String clientId = UUID.randomUUID().toString();  // 고유한 cliendId 생성, Kafka 브로커와의 통신 및 파티션 할당 시 각 컨슈머를 고유하게 식별하는 데 사용
        String key = groupId + "_" + topic + "_" + clientId;  // 고유 key값 생성

        // consumer 생성
        ConsumerFactory<String, String> consumerFactory = consumerConfigCustom.createConsumerFactory(groupId, clientId);

        ContainerProperties containerProps = new ContainerProperties(topic);

        containerProps.setMessageListener(new MessageListener<String, String>() {
            @Override
            public void onMessage(ConsumerRecord<String, String> record) {
                String message = "들어온 메시지 확인 >>>  토픽 : " + record.topic() + ", 메시지 : " + record.value();
                System.out.println(message);
                messageList.add(record.value());
            }
        });

        KafkaMessageListenerContainer<String, String> container = new KafkaMessageListenerContainer<>(consumerFactory, containerProps);
        container.start();
        consumerContainers.put(key, container);
        kafkaMessageList.addAll(messageList);
    }

    public List<String> getAllMessages() {
        return new ArrayList<>(kafkaMessageList);
    }

}
