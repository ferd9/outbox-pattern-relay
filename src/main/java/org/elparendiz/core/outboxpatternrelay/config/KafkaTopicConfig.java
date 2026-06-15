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

    @Bean
    public NewTopic outboxTopic() {
        // Crea el tópico con 1 partición y 1 factor de réplica (ideal para desarrollo local)
        return TopicBuilder.name(topicName)
                .partitions(1)
                .replicas(1)
                .build();
    }
}