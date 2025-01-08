package org.example.serverseminar;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@EnableKafka
public class KafkaDebug {

    // 빈을 직접 가져올 수 있게 도와줌, 빈을 가져올 때는 클래스 타입으로 인자를 넣어줘야 함.
    private final ApplicationContext applicationContext;

    public KafkaDebug(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    @PostConstruct
    public void logKafkaInitialization() {
        KafkaTemplate<String, String> kafkaTemplate = applicationContext.getBean(KafkaTemplate.class);
        System.out.println("\n\nKafka 초기 세팅 설정값");
        System.out.println("KafkaTemplate Bean : " + kafkaTemplate);
        System.out.println("Producer Factory Config : "
                + kafkaTemplate.getProducerFactory().getConfigurationProperties() + "\n\n");
    }

    @PostConstruct
    public void logKafkaProducer() {
        KafkaTemplate<String, String> kafkaTemplate = applicationContext.getBean(KafkaTemplate.class);
        System.out.println("\n\nKafka Producer 관련 설정값");
        System.out.println("ProducerFactory Configuration Properties : "
                + kafkaTemplate.getProducerFactory().getConfigurationProperties() + "\n\n");
    }

    @PostConstruct
    public void logKafkaConsumer() {
        KafkaListenerContainerFactory<?> kafkaListenerContainerFactory = applicationContext.getBean(KafkaListenerContainerFactory.class);
        System.out.println("\n\nKafka Consumer 관련 설정값");
        System.out.println("KafkaListener Container Factory : " + kafkaListenerContainerFactory + "\n\n");
    }
}
