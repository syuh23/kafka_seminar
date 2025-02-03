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

public class ConcurrentKafkaListenerContainerFactoryConfigurerCustom {
    private KafkaProperties properties;
    private BatchMessageConverter batchMessageConverter;
    private RecordMessageConverter recordMessageConverter;
    private RecordFilterStrategy<Object, Object> recordFilterStrategy;
    private KafkaTemplate<Object, Object> replyTemplate;
    private KafkaAwareTransactionManager<Object, Object> transactionManager;
    private ConsumerAwareRebalanceListener rebalanceListener;
    private CommonErrorHandler commonErrorHandler;
    private AfterRollbackProcessor<Object, Object> afterRollbackProcessor;
    private RecordInterceptor<Object, Object> recordInterceptor;
    private BatchInterceptor<Object, Object> batchInterceptor;
    private Function<MessageListenerContainer, String> threadNameSupplier;
    private SimpleAsyncTaskExecutor listenerTaskExecutor;

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


    public void configure(ConcurrentKafkaListenerContainerFactory<Object, Object> listenerFactory,
                          ConsumerFactory<Object, Object> consumerFactory) {
        listenerFactory.setConsumerFactory(consumerFactory);
        configureListenerFactory(listenerFactory);
        configureContainer(listenerFactory.getContainerProperties());
    }

    private void configureListenerFactory(ConcurrentKafkaListenerContainerFactory<Object, Object> factory) {
        PropertyMapper map = PropertyMapper.get().alwaysApplyingWhenNonNull();
        KafkaProperties.Listener properties = this.properties.getListener();
        map.from(properties::getConcurrency).to(factory::setConcurrency);
        map.from(properties::isAutoStartup).to(factory::setAutoStartup);
        map.from(this.batchMessageConverter).to(factory::setBatchMessageConverter);
        map.from(this.recordMessageConverter).to(factory::setRecordMessageConverter);
        map.from(this.recordFilterStrategy).to(factory::setRecordFilterStrategy);
        map.from(this.replyTemplate).to(factory::setReplyTemplate);
        if (properties.getType().equals(KafkaProperties.Listener.Type.BATCH)) {
            factory.setBatchListener(true);
        }
        map.from(this.commonErrorHandler).to(factory::setCommonErrorHandler);
        map.from(this.afterRollbackProcessor).to(factory::setAfterRollbackProcessor);
        map.from(this.recordInterceptor).to(factory::setRecordInterceptor);
        map.from(this.batchInterceptor).to(factory::setBatchInterceptor);
        map.from(this.threadNameSupplier).to(factory::setThreadNameSupplier);
        map.from(properties::getChangeConsumerThreadName).to(factory::setChangeConsumerThreadName);
    }

    private void configureContainer(ContainerProperties container) {
        PropertyMapper map = PropertyMapper.get().alwaysApplyingWhenNonNull();
        KafkaProperties.Listener properties = this.properties.getListener();
        map.from(properties::getAckMode).to(container::setAckMode);
        map.from(properties::getAsyncAcks).to(container::setAsyncAcks);
        map.from(properties::getClientId).to(container::setClientId);
        map.from(properties::getAckCount).to(container::setAckCount);
        map.from(properties::getAckTime).as(Duration::toMillis).to(container::setAckTime);
        map.from(properties::getPollTimeout).as(Duration::toMillis).to(container::setPollTimeout);
        map.from(properties::getNoPollThreshold).to(container::setNoPollThreshold);
        map.from(properties.getIdleBetweenPolls()).as(Duration::toMillis).to(container::setIdleBetweenPolls);
        map.from(properties::getIdleEventInterval).as(Duration::toMillis).to(container::setIdleEventInterval);
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
        map.from(this.transactionManager).to(container::setKafkaAwareTransactionManager);
        map.from(this.rebalanceListener).to(container::setConsumerRebalanceListener);
        map.from(this.listenerTaskExecutor).to(container::setListenerTaskExecutor);
    }
}
