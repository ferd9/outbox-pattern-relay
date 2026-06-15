package org.elparendiz.core.outboxpatternrelay.publisher;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elparendiz.core.outboxpatternrelay.entity.OutboxEvent;
import org.elparendiz.core.outboxpatternrelay.repository.OutboxEventRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Component
@EnableScheduling // Habilita la ejecución de tareas programadas en Spring
@RequiredArgsConstructor
@Slf4j
public class OutboxPublisher {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${kafka.topic.outbox}")
    private String topicName;

    // Se ejecuta cada 5000 milisegundos (5 segundos).
    // Puedes ajustarlo en application.properties con: outbox.polling.interval=3000
    @Scheduled(fixedDelayString = "${outbox.polling.interval:5000}")
    @Transactional
    public void publishPendingEvents() {
        // 1. Buscar eventos pendientes
        List<OutboxEvent> pendingEvents = outboxEventRepository.findByEstado(OutboxEvent.EstadoOutbox.PENDING);

        if (pendingEvents.isEmpty()) {
            return; // No hay nada que hacer
        }

        log.info("Polling Publisher encontró {} eventos pendientes de publicar.", pendingEvents.size());

        for (OutboxEvent event : pendingEvents) {
            try {
                // 2. Publicar en Kafka.
                // Usamos event.getAggregateId().toString() como KEY para garantizar el orden por pedido.
                kafkaTemplate.send(topicName, event.getAggregateId().toString(), event.getPayload())
                        .whenComplete((result, ex) -> {
                            if (ex == null) {
                                log.info("Evento publicado en Kafka: {}", event.getId());
                                // 3. Marcar como PROCESSED solo si Kafka confirma el envío
                                event.setEstado(OutboxEvent.EstadoOutbox.PROCESSED);
                                outboxEventRepository.save(event);
                            } else {
                                log.error("Error al publicar evento {}: {}", event.getId(), ex.getMessage());
                                // Aquí podrías incrementar el contador de reintentos o marcarlo como FAILED
                                event.setReintentos(event.getReintentos() + 1);
                                outboxEventRepository.save(event);
                            }
                        });

                // Nota: En un entorno de alta concurrencia real, aquí se usaría
                // "SELECT ... FOR UPDATE SKIP LOCKED" para evitar que dos instancias
                // procesen el mismo evento al mismo tiempo.

            } catch (Exception e) {
                log.error("Excepción inesperada procesando el evento {}", event.getId(), e);
            }
        }
    }
}