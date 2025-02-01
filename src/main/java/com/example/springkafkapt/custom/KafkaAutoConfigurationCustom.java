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
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder;
import org.springframework.kafka.security.jaas.KafkaJaasLoginModuleInitializer;
import org.springframework.kafka.support.LoggingProducerListener;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.retry.backoff.BackOffPolicyBuilder;
import org.springframework.retry.backoff.SleepingBackOffPolicy;

@Configuration
@ConditionalOnClass({KafkaTemplate.class})
@EnableConfigurationProperties({KafkaProperties.class})
@Import({KafkaAnnotationDrivenConfigurationCustom.class, KafkaStreamsAnnotationDrivenConfigurationCustom.class})  // 이 클래스들에 대한 빈 설정을 먼저 해줘야하기 때문에 이 클래스를 추가적으로 만듦
public class KafkaAutoConfigurationCustom {
    private KafkaProperties properties;

    KafkaAutoConfigurationCustom(KafkaProperties properties) {
        this.properties = properties;
    }

    @Bean
    @ConditionalOnMissingBean({KafkaConnectionDetails.class})
    PropertiesKafkaConnectionDetailsCustom kafkaConnectionDetails(KafkaProperties properties) {
        return new PropertiesKafkaConnectionDetailsCustom(properties);
    }

//    @Bean
//    //@ConditionalOnMissingBean({KafkaTemplate.class})
//    public KafkaTemplate<?, ?> kafkaTemplate(ProducerFactory<Object, Object> kafkaProducerFactory, ProducerListener<Object, Object> kafkaProducerListener, ObjectProvider<RecordMessageConverter> messageConverter) {
//        PropertyMapper map = PropertyMapper.get().alwaysApplyingWhenNonNull();
//        KafkaTemplate<Object, Object> kafkaTemplate = new KafkaTemplate<>(kafkaProducerFactory);
//        messageConverter.ifUnique(kafkaTemplate::setMessageConverter);
//        map.from(kafkaProducerListener).to(kafkaTemplate::setProducerListener);
//        map.from(this.properties.getTemplate().getDefaultTopic()).to(kafkaTemplate::setDefaultTopic);
//        map.from(this.properties.getTemplate().getTransactionIdPrefix()).to(kafkaTemplate::setTransactionIdPrefix);
//        map.from(this.properties.getTemplate().isObservationEnabled()).to(kafkaTemplate::setObservationEnabled);
//        return kafkaTemplate;
//    }

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
    @ConditionalOnMissingBean({ConsumerFactory.class})
    public DefaultKafkaConsumerFactory<?, ?> kafkaConsumerFactory(KafkaConnectionDetails connectionDetails, ObjectProvider<DefaultKafkaConsumerFactoryCustomizer> customizers, ObjectProvider<SslBundles> sslBundles) {
        Map<String, Object> properties = this.properties.buildConsumerProperties((SslBundles)sslBundles.getIfAvailable());
        this.applyKafkaConnectionDetailsForConsumer(properties, connectionDetails);
        DefaultKafkaConsumerFactory<Object, Object> factory = new DefaultKafkaConsumerFactory(properties);
        customizers.orderedStream().forEach((customizer) -> customizer.customize(factory));
        return factory;
    }

    @Bean
    @ConditionalOnMissingBean({ProducerFactory.class})
    public DefaultKafkaProducerFactory<?, ?> kafkaProducerFactory(KafkaConnectionDetails connectionDetails, ObjectProvider<DefaultKafkaProducerFactoryCustomizer> customizers, ObjectProvider<SslBundles> sslBundles) {
        Map<String, Object> properties = this.properties.buildProducerProperties((SslBundles)sslBundles.getIfAvailable());
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
    @ConditionalOnProperty(name = {"spring.kafka.producer.transaction-id-prefix"})
    @ConditionalOnMissingBean
    public KafkaTransactionManager<?, ?> kafkaTransactionManager(ProducerFactory<?, ?> producerFactory) {
        return new KafkaTransactionManager(producerFactory);
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
        Map<String, Object> properties = this.properties.buildAdminProperties((SslBundles)sslBundles.getIfAvailable());
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

    private void applyKafkaConnectionDetailsForConsumer(Map<String, Object> properties, KafkaConnectionDetails connectionDetails) {
        properties.put("bootstrap.servers", connectionDetails.getConsumerBootstrapServers());
        if (!(connectionDetails instanceof PropertiesKafkaConnectionDetailsCustom)) {
            properties.put("security.protocol", "PLAINTEXT");
        }
    }

    private void applyKafkaConnectionDetailsForProducer(Map<String, Object> properties, KafkaConnectionDetails connectionDetails) {
        properties.put("bootstrap.servers", connectionDetails.getProducerBootstrapServers());
        if (!(connectionDetails instanceof PropertiesKafkaConnectionDetailsCustom)) {
            properties.put("security.protocol", "PLAINTEXT");
        }
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
