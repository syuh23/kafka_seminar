package com.example.springkafkapt.custom;
//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate;
import org.springframework.boot.autoconfigure.kafka.*;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.PropertyMapper;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder;
import org.springframework.kafka.security.jaas.KafkaJaasLoginModuleInitializer;
import org.springframework.retry.backoff.BackOffPolicyBuilder;
import org.springframework.retry.backoff.SleepingBackOffPolicy;

@Configuration
@ConditionalOnClass({KafkaTemplate.class})
@EnableConfigurationProperties({KafkaProperties.class})
@Import({KafkaAnnotationDrivenConfigurationCustom.class, KafkaStreamsAnnotationDrivenConfigurationCustom.class})
public class KafkaAutoConfigurationCustom {
    private final KafkaProperties properties;

    KafkaAutoConfigurationCustom(KafkaProperties properties) {
        this.properties = properties;
    }

    @Bean
    @ConditionalOnMissingBean({KafkaConnectionDetails.class})
    PropertiesKafkaConnectionDetailsCustom kafkaConnectionDetails(KafkaProperties properties) {
        return new PropertiesKafkaConnectionDetailsCustom(properties);
    }

    @Bean
    @ConditionalOnProperty(name = {"spring.kafka.jaas.enabled"})
    @ConditionalOnMissingBean
    public KafkaJaasLoginModuleInitializer kafkaJaasInitializer() throws IOException {
        KafkaJaasLoginModuleInitializer jaas = new KafkaJaasLoginModuleInitializer();
        KafkaProperties.Jaas jaasProperties = this.properties.getJaas();
        if (jaasProperties.getControlFlag() != null) {
            jaas.setControlFlag(jaasProperties.getControlFlag());
        }
        if (jaasProperties.getLoginModule() != null) {
            jaas.setLoginModule(jaasProperties.getLoginModule());
        }
        jaas.setOptions(jaasProperties.getOptions());
        return jaas;
    }

    @Bean
    @ConditionalOnMissingBean
    public KafkaAdmin kafkaAdmin(KafkaConnectionDetails connectionDetails, ObjectProvider<SslBundles> sslBundles) {
        Map<String, Object> properties = this.properties.buildAdminProperties(sslBundles.getIfAvailable());
        this.applyKafkaConnectionDetailsForAdmin(properties, connectionDetails);
        KafkaAdmin kafkaAdmin = new KafkaAdmin(properties);
        KafkaProperties.Admin admin = this.properties.getAdmin();
        if (admin.getCloseTimeout() != null) {
            kafkaAdmin.setCloseTimeout((int)admin.getCloseTimeout().getSeconds());
        }
        if (admin.getOperationTimeout() != null) {
            kafkaAdmin.setOperationTimeout((int)admin.getOperationTimeout().getSeconds());
        }
        kafkaAdmin.setFatalIfBrokerNotAvailable(admin.isFailFast());
        kafkaAdmin.setModifyTopicConfigs(admin.isModifyTopicConfigs());
        kafkaAdmin.setAutoCreate(admin.isAutoCreate());
        return kafkaAdmin;
    }

    @Bean
    @ConditionalOnProperty(name = {"spring.kafka.retry.topic.enabled"})
    @ConditionalOnSingleCandidate(KafkaTemplate.class)
    public RetryTopicConfiguration kafkaRetryTopicConfiguration(KafkaTemplate<?, ?> kafkaTemplate) {
        KafkaProperties.Retry.Topic retryTopic = this.properties.getRetry().getTopic();
        RetryTopicConfigurationBuilder builder = RetryTopicConfigurationBuilder.newInstance().maxAttempts(retryTopic.getAttempts()).useSingleTopicForSameIntervals().suffixTopicsWithIndexValues().doNotAutoCreateRetryTopics();
        setBackOffPolicy(builder, retryTopic.getBackoff());
        return builder.create(kafkaTemplate);
    }

    private void applyKafkaConnectionDetailsForAdmin(Map<String, Object> properties, KafkaConnectionDetails connectionDetails) {
        properties.put("bootstrap.servers", connectionDetails.getAdminBootstrapServers());
        if (!(connectionDetails instanceof PropertiesKafkaConnectionDetailsCustom)) {
            properties.put("security.protocol", "PLAINTEXT");
        }
    }

    private static void setBackOffPolicy(RetryTopicConfigurationBuilder builder, KafkaProperties.Retry.Topic.Backoff retryTopicBackoff) {
        long delay = (retryTopicBackoff.getDelay() != null) ? retryTopicBackoff.getDelay().toMillis() : 0;
        if (delay > 0) {
            PropertyMapper map = PropertyMapper.get().alwaysApplyingWhenNonNull();
            BackOffPolicyBuilder backOffPolicy = BackOffPolicyBuilder.newBuilder();
            map.from(delay).to(backOffPolicy::delay);
            map.from(retryTopicBackoff.getMaxDelay()).as(Duration::toMillis).to(backOffPolicy::maxDelay);
            map.from(retryTopicBackoff.getMultiplier()).to(backOffPolicy::multiplier);
            map.from(retryTopicBackoff.isRandom()).to(backOffPolicy::random);
            builder.customBackoff((SleepingBackOffPolicy<?>) backOffPolicy.build());
        }
        else {
            builder.noBackoff();
        }

    }
}
