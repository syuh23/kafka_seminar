package com.example.springkafkapt.config;

import com.example.springkafkapt.custom.PropertiesKafkaConnectionDetailsCustom;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.boot.autoconfigure.kafka.KafkaConnectionDetails;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.LoggingProducerListener;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.kafka.transaction.KafkaTransactionManager;

import java.util.Map;

@Configuration
@ConditionalOnClass({KafkaTemplate.class})
@EnableConfigurationProperties({KafkaProperties.class})
@Profile("producer")
public class ProducerConfig {
    private final KafkaProperties properties;

    public ProducerConfig(KafkaProperties kafkaProperties) {
        this.properties = kafkaProperties;
    }

    @Bean
    @ConditionalOnMissingBean({ProducerFactory.class})
    public DefaultKafkaProducerFactory<?, ?> kafkaProducerFactory(KafkaConnectionDetails connectionDetails,
                                                                  ObjectProvider<DefaultKafkaProducerFactoryCustomizer> customizers,
                                                                  ObjectProvider<SslBundles> sslBundles) {
        Map<String, Object> properties = this.properties.buildProducerProperties(sslBundles.getIfAvailable());
        this.applyKafkaConnectionDetailsForProducer(properties, connectionDetails);
        DefaultKafkaProducerFactory<?, ?> factory = new DefaultKafkaProducerFactory(properties);
        String transactionIdPrefix = this.properties.getProducer().getTransactionIdPrefix();
        if (transactionIdPrefix != null) {
            factory.setTransactionIdPrefix(transactionIdPrefix);
        }
        customizers.orderedStream().forEach((customizer) -> customizer.customize(factory));
        return factory;
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(producerFactory);
        return kafkaTemplate;
    }

    @Bean
    @ConditionalOnMissingBean({ProducerListener.class})
    public LoggingProducerListener<Object, Object> kafkaProducerListener() {
        return new LoggingProducerListener();
    }

    @Bean
    @ConditionalOnProperty(name = {"spring.kafka.producer.transaction-id-prefix"})
    @ConditionalOnMissingBean
    public KafkaTransactionManager<?, ?> kafkaTransactionManager(ProducerFactory<?, ?> producerFactory) {
        return new KafkaTransactionManager(producerFactory);
    }

    private void applyKafkaConnectionDetailsForProducer(Map<String, Object> properties, KafkaConnectionDetails connectionDetails) {
        properties.putIfAbsent("bootstrap.servers", connectionDetails.getProducerBootstrapServers());
        if (!(connectionDetails instanceof PropertiesKafkaConnectionDetailsCustom)) {
            properties.put("security.protocol", "PLAINTEXT");
        }
    }

}
