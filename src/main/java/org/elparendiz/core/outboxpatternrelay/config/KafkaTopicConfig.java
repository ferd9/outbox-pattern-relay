package org.elparendiz.core.outboxpatternrelay.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Value("${kafka.topic.outbox}")
    private String topicName;

    @Value("${kafka.topic.dlq}") // Asegúrate de tener esta propiedad en tu application.properties
    private String dlqTopicName;

    @Bean
    public NewTopic outboxTopic() {
        return TopicBuilder.name(topicName)
                .partitions(1)
                .replicas(1)
                .build();
    }

    // NUEVO: Bean para crear el tópico de la Dead Letter Queue
    @Bean
    public NewTopic dlqTopic() {
        return TopicBuilder.name(dlqTopicName)
                .partitions(1)
                .replicas(1)
                .build();
    }
}