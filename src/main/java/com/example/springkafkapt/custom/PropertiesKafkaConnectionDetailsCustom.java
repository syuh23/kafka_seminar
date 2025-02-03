package com.example.springkafkapt.custom;

import org.springframework.boot.autoconfigure.kafka.KafkaConnectionDetails;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

import java.util.List;

public class PropertiesKafkaConnectionDetailsCustom implements KafkaConnectionDetails {
    private final KafkaProperties properties;

    public PropertiesKafkaConnectionDetailsCustom(KafkaProperties properties) {
        this.properties = properties;
    }

    @Override
    public List<String> getBootstrapServers() {
        return this.properties.getBootstrapServers();
    }

    @Override
    public List<String> getConsumerBootstrapServers() {
        return this.getServers(this.properties.getConsumer().getBootstrapServers());
    }

    @Override
    public List<String> getProducerBootstrapServers() {
        return this.getServers(this.properties.getProducer().getBootstrapServers());
    }

    @Override
    public List<String> getStreamsBootstrapServers() {
        return this.getServers(this.properties.getStreams().getBootstrapServers());
    }

    private List<String> getServers(List<String> servers) {
        return servers != null ? servers : this.getBootstrapServers();
    }
}
