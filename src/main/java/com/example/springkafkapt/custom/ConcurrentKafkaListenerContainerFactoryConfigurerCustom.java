package com.example.springkafkapt.custom;

import java.time.Duration;
import java.util.function.Function;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.PropertyMapper;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AfterRollbackProcessor;
import org.springframework.kafka.listener.BatchInterceptor;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;
import org.springframework.kafka.support.converter.BatchMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.transaction.KafkaAwareTransactionManager;

// 이름에서 알 수 있듯이, ConcurrentKafkaListenerContainerFactory를 설정하는 역할을 하는 클래스
// ConcurrentKafkaListenerContainerFactory?? SpringKafka에서 Kafka 메시지를 소비하는 리스너를 생성하는 팩토리
// 컨테이너 팩토리를 여기서 설정하면 메시지를 처리하는 방법, 오류 처리 방식, 트랜잭션 관리 등을 제어할 수 있음
public class ConcurrentKafkaListenerContainerFactoryConfigurerCustom {
    private KafkaProperties properties; // application.properties에 적힌 Kafka 관련 속성 가져오는 객체
    private BatchMessageConverter batchMessageConverter; // 메시지 변환기 - 배치로 들어오는 메시지 반환
    private RecordMessageConverter recordMessageConverter; // 메시지 변환기 - 단일 메시지 반환
    private RecordFilterStrategy<Object, Object> recordFilterStrategy;
    private KafkaTemplate<Object, Object> replyTemplate;
    private KafkaAwareTransactionManager<Object, Object> transactionManager; // 메시지를 처리할 때 트랜잭션을 관리하는 객체, 메시지를 처리하는 동안 트랜잭션 적용, 문제 발생 시 롤백 가능
    private ConsumerAwareRebalanceListener rebalanceListener; // 파티션 리밸런싱이 일어났을 때 동작을 정의하는 리스너, 파티션 할당이 변경될 때 리소스를 정리하거나 초기화하는 로직 추가 가능
    private CommonErrorHandler commonErrorHandler; // 메시지 소비 중 오류가 발생했을 때의 동작을 정의 (오류가 발생한 메시지를 다시 처리, 바로 무시 등의 전략 설정)
    private AfterRollbackProcessor<Object, Object> afterRollbackProcessor;
    private RecordInterceptor<Object, Object> recordInterceptor; // 메시지 소비하기 전후에 실행되는 인터셉터(필터) - 단일 메시지에 대해 동작
    private BatchInterceptor<Object, Object> batchInterceptor; // 메시지 소비하기 전후에 실행되는 인터셉터(필터) - 배치 메시지에 대해 동작
    private Function<MessageListenerContainer, String> threadNameSupplier; // 카프카 리스너 컨테이너가 실행되는 스레드의 이름을 지정하는 함수
    private SimpleAsyncTaskExecutor listenerTaskExecutor; // 카프카 메시지를 소비하는 리스너 스레드를 관리하는 실행자 (executor)

    public ConcurrentKafkaListenerContainerFactoryConfigurerCustom() {
    }

    void setKafkaProperties(KafkaProperties properties) {
        this.properties = properties;
    }

    void setBatchMessageConverter(BatchMessageConverter batchMessageConverter) {
        this.batchMessageConverter = batchMessageConverter;
    }

    void setRecordMessageConverter(RecordMessageConverter recordMessageConverter) {
        this.recordMessageConverter = recordMessageConverter;
    }

    void setRecordFilterStrategy(RecordFilterStrategy<Object, Object> recordFilterStrategy) {
        this.recordFilterStrategy = recordFilterStrategy;
    }

    void setReplyTemplate(KafkaTemplate<Object, Object> replyTemplate) {
        this.replyTemplate = replyTemplate;
    }

    void setTransactionManager(KafkaAwareTransactionManager<Object, Object> transactionManager) {
        this.transactionManager = transactionManager;
    }

    void setRebalanceListener(ConsumerAwareRebalanceListener rebalanceListener) {
        this.rebalanceListener = rebalanceListener;
    }

    public void setCommonErrorHandler(CommonErrorHandler commonErrorHandler) {
        this.commonErrorHandler = commonErrorHandler;
    }

    void setAfterRollbackProcessor(AfterRollbackProcessor<Object, Object> afterRollbackProcessor) {
        this.afterRollbackProcessor = afterRollbackProcessor;
    }

    void setRecordInterceptor(RecordInterceptor<Object, Object> recordInterceptor) {
        this.recordInterceptor = recordInterceptor;
    }

    void setBatchInterceptor(BatchInterceptor<Object, Object> batchInterceptor) {
        this.batchInterceptor = batchInterceptor;
    }

    void setThreadNameSupplier(Function<MessageListenerContainer, String> threadNameSupplier) {
        this.threadNameSupplier = threadNameSupplier;
    }

    void setListenerTaskExecutor(SimpleAsyncTaskExecutor listenerTaskExecutor) {
        this.listenerTaskExecutor = listenerTaskExecutor;
    }

    // 컨테이너 팩토리와 컨슈머 팩토리를 설정하는 메서드, 얘가 핵심임 !!!
    public void configure(ConcurrentKafkaListenerContainerFactory<Object, Object> listenerFactory,
                          ConsumerFactory<Object, Object> consumerFactory) {
        listenerFactory.setConsumerFactory(consumerFactory); // 리스너 팩토리에 컨슈머 팩토리를 연결
        configureListenerFactory(listenerFactory); // 밑에 함수
        configureContainer(listenerFactory.getContainerProperties()); // 그 밑에 함수
    }

    // ConcurrentKafkaListenerContainerFactory를 설정하는 메서드
    private void configureListenerFactory(ConcurrentKafkaListenerContainerFactory<Object, Object> factory) {
        // PropertyMapper를 사용해서 KafkaProperties의 속성을 팩토리로 매핑함
        PropertyMapper map = PropertyMapper.get().alwaysApplyingWhenNonNull();
        KafkaProperties.Listener properties = this.properties.getListener();
        map.from(properties::getConcurrency).to(factory::setConcurrency); // Concurrency : 리스너의 스레드 개수
        map.from(properties::isAutoStartup).to(factory::setAutoStartup); // AutoStartup : 리스너가 자동으로 시작할지 여부
        map.from(this.batchMessageConverter).to(factory::setBatchMessageConverter);
        map.from(this.recordMessageConverter).to(factory::setRecordMessageConverter);
        map.from(this.recordFilterStrategy).to(factory::setRecordFilterStrategy);
        map.from(this.replyTemplate).to(factory::setReplyTemplate);
        if (properties.getType().equals(KafkaProperties.Listener.Type.BATCH)) {
            factory.setBatchListener(true); // BatchListener : 배치 메시지를 처리할지 여부
        }
        map.from(this.commonErrorHandler).to(factory::setCommonErrorHandler); // 오류 처리기
        map.from(this.afterRollbackProcessor).to(factory::setAfterRollbackProcessor);
        map.from(this.recordInterceptor).to(factory::setRecordInterceptor); // 인터셉터
        map.from(this.batchInterceptor).to(factory::setBatchInterceptor); // 인터셉터
        map.from(this.threadNameSupplier).to(factory::setThreadNameSupplier);
        map.from(properties::getChangeConsumerThreadName).to(factory::setChangeConsumerThreadName);
    }

    // Kafka 메시지를 소비하는 컨테이너(ContainerProperties)의 속성을 설정
    private void configureContainer(ContainerProperties container) {
        PropertyMapper map = PropertyMapper.get().alwaysApplyingWhenNonNull(); // 또 매핑하나봄
        KafkaProperties.Listener properties = this.properties.getListener();
        map.from(properties::getAckMode).to(container::setAckMode); // AckMode : 메시지 확인 모드
        map.from(properties::getAsyncAcks).to(container::setAsyncAcks);
        map.from(properties::getClientId).to(container::setClientId);
        map.from(properties::getAckCount).to(container::setAckCount);
        map.from(properties::getAckTime).as(Duration::toMillis).to(container::setAckTime);
        map.from(properties::getPollTimeout).as(Duration::toMillis).to(container::setPollTimeout); // PollTimeout : 메시지를 가져올 때의 타임아웃
        map.from(properties::getNoPollThreshold).to(container::setNoPollThreshold);
        map.from(properties.getIdleBetweenPolls()).as(Duration::toMillis).to(container::setIdleBetweenPolls);
        map.from(properties::getIdleEventInterval).as(Duration::toMillis).to(container::setIdleEventInterval); // IdleEventInterval : 컨슈머가 유휴 상태일 때 발생하는 이벤트 간격 ////??????
        map.from(properties::getIdlePartitionEventInterval)
                .as(Duration::toMillis)
                .to(container::setIdlePartitionEventInterval);
        map.from(properties::getMonitorInterval)
                .as(Duration::getSeconds)
                .as(Number::intValue)
                .to(container::setMonitorInterval);
        map.from(properties::getLogContainerConfig).to(container::setLogContainerConfig);
        map.from(properties::isMissingTopicsFatal).to(container::setMissingTopicsFatal);
        map.from(properties::isImmediateStop).to(container::setStopImmediate);
        map.from(properties::isObservationEnabled).to(container::setObservationEnabled);
        map.from(this.transactionManager).to(container::setKafkaAwareTransactionManager); // 트랜잭션 관리
        map.from(this.rebalanceListener).to(container::setConsumerRebalanceListener); // 리밸런싱 리스너
        map.from(this.listenerTaskExecutor).to(container::setListenerTaskExecutor); // 스레드 실행자
    }
}
