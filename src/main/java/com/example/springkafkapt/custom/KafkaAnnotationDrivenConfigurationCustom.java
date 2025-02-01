package com.example.springkafkapt.custom;

import java.util.function.Function;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnThreading;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;  // 내가 커스텀 한 클래스임
import org.springframework.boot.autoconfigure.thread.Threading;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.config.KafkaListenerConfigUtils;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AfterRollbackProcessor;
import org.springframework.kafka.listener.BatchInterceptor;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;
import org.springframework.kafka.support.converter.BatchMessageConverter;
import org.springframework.kafka.support.converter.BatchMessagingMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.transaction.KafkaAwareTransactionManager;

// 여기 오류들은 ConcurrentKafkaListenerContainerFactoryConfigurer 만들면 끝남

@Configuration(proxyBeanMethods = false) // 스프링 설정 클래스 (proxyBeanMethods : 프록시를 사용하지 않아 메모리를 아낄 수 있도록 최적화한 설정)
@ConditionalOnClass({EnableKafka.class}) // EnableKafka 클래스가 클래스패스에 존재할 때만 이 설정이 적용됨. 즉, Kafka 관련 의존성이 추가되지 않았다면 이 클래스 무시
    // 클래스패스?? 애플리케이션이 실행될 때 사용할 수 있는 클래스 파일과 리소스 파일의 경로 목록 (내 프로젝트 패키지 + 외부 라이브러리까지 모두 포함)
    // 즉, ConditionalOnClass({EnableKafka.class}) => 스프링 카프카 의존성이 프로젝트에 포함되었는가? 를 확인하는 조건 !!!
class KafkaAnnotationDrivenConfigurationCustom {
    private final KafkaProperties properties;
    private final RecordMessageConverter recordMessageConverter;
    private final RecordFilterStrategy<Object, Object> recordFilterStrategy;
    private final BatchMessageConverter batchMessageConverter;
    private final KafkaTemplate<Object, Object> kafkaTemplate;
    private final KafkaAwareTransactionManager<Object, Object> transactionManager;
    private final ConsumerAwareRebalanceListener rebalanceListener;
    private final CommonErrorHandler commonErrorHandler;
    private final AfterRollbackProcessor<Object, Object> afterRollbackProcessor;
    private final RecordInterceptor<Object, Object> recordInterceptor;
    private final BatchInterceptor<Object, Object> batchInterceptor;
    private final Function<MessageListenerContainer, String> threadNameSupplier;

    KafkaAnnotationDrivenConfigurationCustom(KafkaProperties properties,
                                       ObjectProvider<RecordMessageConverter> recordMessageConverter,
                                       ObjectProvider<RecordFilterStrategy<Object, Object>> recordFilterStrategy,
                                       ObjectProvider<BatchMessageConverter> batchMessageConverter,
                                       ObjectProvider<KafkaTemplate<Object, Object>> kafkaTemplate,
                                       ObjectProvider<KafkaAwareTransactionManager<Object, Object>> kafkaTransactionManager,
                                       ObjectProvider<ConsumerAwareRebalanceListener> rebalanceListener,
                                       ObjectProvider<CommonErrorHandler> commonErrorHandler,
                                       ObjectProvider<AfterRollbackProcessor<Object, Object>> afterRollbackProcessor,
                                       ObjectProvider<RecordInterceptor<Object, Object>> recordInterceptor,
                                       ObjectProvider<BatchInterceptor<Object, Object>> batchInterceptor,
                                       ObjectProvider<Function<MessageListenerContainer, String>> threadNameSupplier) {
        // 매개변수의 ObjectProvider : 스프링에서 제공하는 지연 로딩 도구, 실제로 필요한 시점에 빈을 가져올 수 있게 해줌
        // 아래의 .getIfUnique() : 특정 빈이 유일하게 존재할 경우만 가져오도록 함 (유일하게 존재할 경우?? = 해당 타입의 빈이 하나만 존재할 경우)
        this.properties = properties; // application.properties에 작성된 Kafka 관련 설정 관리
        this.recordMessageConverter = recordMessageConverter.getIfUnique(); // Kafka 메시지를 변환
        this.recordFilterStrategy = recordFilterStrategy.getIfUnique();
        this.batchMessageConverter = batchMessageConverter // 배치 메시지 처리에 사용 (배치?? = 데이터를 한 번에 여러 개씩 묶어서 처리하는 방식)
                .getIfUnique(() -> new BatchMessagingMessageConverter(this.recordMessageConverter));
        this.kafkaTemplate = kafkaTemplate.getIfUnique(); // 메시지를 보내기 위한 템플릿
        this.transactionManager = kafkaTransactionManager.getIfUnique();
        this.rebalanceListener = rebalanceListener.getIfUnique();
        this.commonErrorHandler = commonErrorHandler.getIfUnique(); // 메시지 처리 중 발생한 오류를 다룸
        this.afterRollbackProcessor = afterRollbackProcessor.getIfUnique();
        this.recordInterceptor = recordInterceptor.getIfUnique();
        this.batchInterceptor = batchInterceptor.getIfUnique();
        this.threadNameSupplier = threadNameSupplier.getIfUnique();
    }

    @Bean // 해당 메서드가 반환하는 객체를 빈으로 등록
    @ConditionalOnMissingBean // 동일한 타입의 빈이 이미 등록되어 있지 않은 경우에만 이 빈을 생성
    @ConditionalOnThreading(Threading.PLATFORM) // PLATFORM 스레드가 사용될 때만 빈이 생성됨   ///////??????????????????
    ConcurrentKafkaListenerContainerFactoryConfigurerCustom kafkaListenerContainerFactoryConfigurer() {
        return configurer(); // 밑에 configurer() 메소드의 반환값이 최종적으로 Bean으로 등록됨
        // 반환값인 ConcurrentKafkaListenerCOntainerFactoryConfigurer(타입)는 Kafka 리스너 컨테이너를 설정하는 데 사용됨
    }

    @Bean  //(name = "kafkaListenerContainerFactoryConfigurer")  // 빈 이름 설정 ??????????????
    @ConditionalOnMissingBean
    @ConditionalOnThreading(Threading.VIRTUAL)
    ConcurrentKafkaListenerContainerFactoryConfigurerCustom kafkaListenerContainerFactoryConfigurerVirtualThreads() {
        ConcurrentKafkaListenerContainerFactoryConfigurerCustom configurer = configurer();
        SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor("kafka-");
        executor.setVirtualThreads(true);
        configurer.setListenerTaskExecutor(executor);
        return configurer;
    }

    private ConcurrentKafkaListenerContainerFactoryConfigurerCustom configurer() {
        ConcurrentKafkaListenerContainerFactoryConfigurerCustom configurer = new ConcurrentKafkaListenerContainerFactoryConfigurerCustom();
        // Kafka 관련 설정들을 적용 (배치 처리, 트랜잭션, 오류 처리, 메시지 변환기 등의 설정이 여기서 정의됨)
        configurer.setKafkaProperties(this.properties);
        configurer.setBatchMessageConverter(this.batchMessageConverter);
        configurer.setRecordMessageConverter(this.recordMessageConverter);
        configurer.setRecordFilterStrategy(this.recordFilterStrategy);
        configurer.setReplyTemplate(this.kafkaTemplate);
        configurer.setTransactionManager(this.transactionManager);
        configurer.setRebalanceListener(this.rebalanceListener);
        configurer.setCommonErrorHandler(this.commonErrorHandler);
        configurer.setAfterRollbackProcessor(this.afterRollbackProcessor);
        configurer.setRecordInterceptor(this.recordInterceptor);
        configurer.setBatchInterceptor(this.batchInterceptor);
        configurer.setThreadNameSupplier(this.threadNameSupplier);
        return configurer;
    }

    @Bean
    @ConditionalOnMissingBean  //(name = "kafkaListenerContainerFactory")  // 빈 이름 설정 ?????????????
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurerCustom configurer,
            ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory,
            ObjectProvider<ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>>> kafkaContainerCustomizer,
            ObjectProvider<SslBundles> sslBundles) {
        // 카프카 리스너 컨테이너 팩토리를 생성하는 클래스, 리스너가 Kafka 메시지를 소비할 때 필요한 설정을 포함함
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory.getIfAvailable(() -> new DefaultKafkaConsumerFactory<>(
                this.properties.buildConsumerProperties(sslBundles.getIfAvailable()))));
        kafkaContainerCustomizer.ifAvailable(factory::setContainerCustomizer);
        return factory;
    }

    @Configuration(proxyBeanMethods = false)
    @EnableKafka  // Kafka 리스너를 활성화하기 위해 필요한 설정을 자동으로 추가
    @ConditionalOnMissingBean(name = KafkaListenerConfigUtils.KAFKA_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME)  // 특정 빈이 없을 경우에만 활성화
    static class EnableKafkaConfiguration {
        // 내부 클래스
        // Kafka 리스너와 관련된 기본 설정을 추가적으로 등록해줌
    }
}