package com.example.springkafkapt.config;

import com.example.springkafkapt.custom.PropertiesKafkaConnectionDetailsCustom;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.DefaultKafkaConsumerFactoryCustomizer;
import org.springframework.boot.autoconfigure.kafka.KafkaConnectionDetails;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Map;

@Configuration
@ConditionalOnClass({KafkaTemplate.class})
@EnableConfigurationProperties({KafkaProperties.class})
@Profile({"consumer-one", "consumer-two"})
public class ConsumerConfig {
    private final KafkaProperties properties;

    public ConsumerConfig(KafkaProperties properties) {
        this.properties = properties;
    }

    @Bean
    @ConditionalOnMissingBean({ConsumerFactory.class})
    public DefaultKafkaConsumerFactory<?, ?> kafkaConsumerFactory(KafkaConnectionDetails connectionDetails,
                                                                  ObjectProvider<DefaultKafkaConsumerFactoryCustomizer> customizers,
                                                                  ObjectProvider<SslBundles> sslBundles) {
        Map<String, Object> properties = this.properties.buildConsumerProperties(sslBundles.getIfAvailable());
        this.applyKafkaConnectionDetailsForConsumer(properties, connectionDetails);
        DefaultKafkaConsumerFactory<Object, Object> factory = new DefaultKafkaConsumerFactory(properties);
        customizers.orderedStream().forEach((customizer) -> customizer.customize(factory));
        return factory;
    }

    private void applyKafkaConnectionDetailsForConsumer(Map<String, Object> properties, KafkaConnectionDetails connectionDetails) {
        properties.putIfAbsent("bootstrap.servers", connectionDetails.getConsumerBootstrapServers());
        if (!(connectionDetails instanceof PropertiesKafkaConnectionDetailsCustom)) {
            properties.put("security.protocol", "PLAINTEXT");
        }
    }

}
