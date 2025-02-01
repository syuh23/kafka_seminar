package com.example.springkafkapt.custom;

import java.util.Map;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.KafkaConnectionDetails;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.autoconfigure.kafka.StreamsBuilderFactoryBeanCustomizer;
import org.springframework.boot.context.properties.source.InvalidConfigurationPropertyValueException;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.CleanupConfig;

// @EnableKafkaStreams 어노테이션과 관련된 설정을 처리하는 중요한 클래스
// Kafka Streams 애플리케이션의 자동 설정 및 초기화를 지원, Kafka Streams의 설정, 팩토리 빈 생성, 커스터마이징 등을 관리함
@EnableKafkaStreams
@Configuration(proxyBeanMethods = false)  // proxyBeanMethods : 빈 메서드 호출 간 프록시를 생성하지 않도록 최적화
@ConditionalOnClass(StreamsBuilder.class)  // 클래스패스에 StreamsBuilder가 있을 때만 이 클래스가 활성화 됨 (KafkaAnnotationDrivenConfiguration이랑 똑같음)
@ConditionalOnBean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_BUILDER_BEAN_NAME)  // 괄호 안의 빈이 애플리케이션 컨텍스트에 등록되어 있어야 이 클래스가 활성화 됨
class KafkaStreamsAnnotationDrivenConfigurationCustom {

    private final KafkaProperties properties;

    KafkaStreamsAnnotationDrivenConfigurationCustom(KafkaProperties properties) {
        this.properties = properties;
    }

    // Kafka Streams 설정을 담은 KafkaStreamsConfiguration 객체를 생성함
    @ConditionalOnMissingBean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)  // DEFAULT_STREAMS_CONFIG_BEAN_NAME 빈이 없을 경우에만 실행됨
    @Bean(KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)  // 생성함
    public KafkaStreamsConfiguration defaultKafkaStreamsConfig(Environment environment,
                                                        KafkaConnectionDetails connectionDetails, ObjectProvider<SslBundles> sslBundles) {

        // KafkaProperties에서 Kafka Streams 프로퍼티를 가져와 설정함
        Map<String, Object> properties = this.properties.buildStreamsProperties(sslBundles.getIfAvailable());
        applyKafkaConnectionDetailsForStreams(properties, connectionDetails); // 밑에 함수

        // Application-id 설정 (Application-id : Kafka Streams 애플리케이션의 고유 식별자, 필수 설정!!
        if (this.properties.getStreams().getApplicationId() == null) {  // Application-id가 없는 경우
            String applicationName = environment.getProperty("spring.application.name"); // application.name을 fallback으로 사용

            if (applicationName == null) { // 둘 다 없으면 예외를 던짐
                throw new InvalidConfigurationPropertyValueException("spring.kafka.streams.application-id", null,
                        "This property is mandatory and fallback 'spring.application.name' is not set either.");
            }
            properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationName);
        }
        return new KafkaStreamsConfiguration(properties);
    }

    // StreamsBuilderFactoryBean을 커스터마이징
    @Bean
    KafkaStreamsFactoryBeanConfigurer kafkaStreamsFactoryBeanConfigurer(
            @Qualifier(KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_BUILDER_BEAN_NAME) StreamsBuilderFactoryBean factoryBean,
            ObjectProvider<StreamsBuilderFactoryBeanCustomizer> customizers) {  // 이 커스터마이저를 통해 팩토리 빈을 유연하게 설정할 수 있음
        customizers.orderedStream().forEach((customizer) -> customizer.customize(factoryBean));
        return new KafkaStreamsFactoryBeanConfigurer(this.properties, factoryBean);
    }

    // Kafka 연결 정보를 Streams 설정에 추가
    private void applyKafkaConnectionDetailsForStreams(Map<String, Object> properties,
                                                       KafkaConnectionDetails connectionDetails) {
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connectionDetails.getStreamsBootstrapServers());
        if (!(connectionDetails instanceof PropertiesKafkaConnectionDetailsCustom)) {
            properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
        }
    }

    // 내부 클래스
    static class KafkaStreamsFactoryBeanConfigurer implements InitializingBean {
        // InitializingBean : Spring Bean의 모든 프로퍼티가 설정된 후 호츨되는 메서드

        private final KafkaProperties properties;
        private final StreamsBuilderFactoryBean factoryBean;

        KafkaStreamsFactoryBeanConfigurer(KafkaProperties properties, StreamsBuilderFactoryBean factoryBean) {
            this.properties = properties;
            this.factoryBean = factoryBean;
        }

        @Override
        public void afterPropertiesSet() {
            // 카프카 스트림 팩토리 빈의 자동 시작 여부를 설정
            this.factoryBean.setAutoStartup(this.properties.getStreams().isAutoStartup());

            // Kafka Streams의 상태 저장소(Cleanup) 설정
            KafkaProperties.Cleanup cleanup = this.properties.getStreams().getCleanup();
            CleanupConfig cleanupConfig = new CleanupConfig(cleanup.isOnStartup(), cleanup.isOnShutdown());
            // OnStartup : 애플리케이션 시작 시 상태 저장소 초기화, OnShutdowm : 애플리케이션 종료 시 상태 저장소 초기화
            this.factoryBean.setCleanupConfig(cleanupConfig);
        }

    }


    // Kafka Streams Config 내가 작성한 파일에 있던거 걍 여기다가 갖다 붙임
    @Bean(name = "myCustomKafkaStreamsBuilder")
    public StreamsBuilder customStreamsBuilder() {
        return new StreamsBuilder(); // 필요하다면 StreamsBuilder 초기화 로직 추가
    }

    @Bean
    public KStream<String, String> kStream(@Qualifier("myCustomKafkaStreamsBuilder")StreamsBuilder streamsBuilder) {
        // 입력 토픽 설정
        KStream<String, String> stream = streamsBuilder.stream("syuh-input-topic");

        stream.foreach((key, value) -> {
            System.out.println("Received message: key = " + key + ", value = " + value);
        });

        return stream;
    }
}