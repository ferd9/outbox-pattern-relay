package org.elparendiz.core.outboxpatternrelay.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elparendiz.core.outboxpatternrelay.entity.ConsumedEvent;
import org.elparendiz.core.outboxpatternrelay.repository.ConsumedEventRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class InventoryConsumer {

    private final ConsumedEventRepository consumedEventRepository;
    private final ObjectMapper objectMapper;

    // Escuchamos el tópico que Debezium está alimentando
    @KafkaListener(topics = "outbox-events-topic", groupId = "inventory-service-group")
    @Transactional
    public void consumeEvent(String message) {
        try {
            // 1. Parsear el envelope de Debezium
            JsonNode root = objectMapper.readTree(message);
            JsonNode after = root.get("after");

            if (after == null) {
                log.warn("Mensaje ignorado (operación DELETE o sin datos 'after')");
                return;
            }

            // 2. Extraer el ID del evento Outbox (para idempotencia)
            String outboxEventIdStr = after.get("id").asText();
            UUID outboxEventId = UUID.fromString(outboxEventIdStr);

            // 3. VERIFICAR IDEMPOTENCIA: ¿Ya procesamos este evento?
            if (consumedEventRepository.existsByOutboxEventId(outboxEventId)) {
                log.info("️ Evento DUPLICADO detectado (ID: {}). Ignorando para evitar reprocesamiento.", outboxEventId);
                return;
            }

            // 4. Extraer los datos de negocio (el payload que guardamos en Spring Boot)
            // Nota: Debezium nos da el campo 'payload' de la tabla outbox_events como un String JSON
            String businessPayloadStr = after.get("payload").asText();
            JsonNode businessPayload = objectMapper.readTree(businessPayloadStr);

            String pedidoId = businessPayload.get("pedidoId").asText();
            String cliente = businessPayload.get("cliente").asText();
            double total = businessPayload.get("total").asDouble();

            // 5. EJECUTAR LÓGICA DE NEGOCIO (Simulación de Inventario)
            log.info("[INVENTARIO] Procesando pedido {} para cliente {}. Total: ${}", pedidoId, cliente, total);
            log.info("[INVENTARIO] Stock reservado exitosamente para el pedido {}", pedidoId);

            // 6. GUARDAR REGISTRO DE IDEMPOTENCIA
            ConsumedEvent consumedEvent = ConsumedEvent.builder()
                    .outboxEventId(outboxEventId)
                    .aggregateId(UUID.fromString(pedidoId))
                    .processedAt(LocalDateTime.now())
                    .build();

            consumedEventRepository.save(consumedEvent);
            log.info("Evento {} marcado como procesado en la base de datos.", outboxEventId);

        } catch (Exception e) {
            log.error("Error fatal procesando el mensaje de Kafka: {}", e.getMessage(), e);
            // En producción, aquí enviarías el mensaje a una Dead Letter Queue (DLQ)
        }
    }
}
