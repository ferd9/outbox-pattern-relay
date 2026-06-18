package org.elparendiz.core.outboxpatternrelay.consumer;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elparendiz.core.outboxpatternrelay.entity.ConsumedEvent;
import org.elparendiz.core.outboxpatternrelay.repository.ConsumedEventRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
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
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${kafka.topic.dlq}")
    private String dlqTopicName;

    /*@KafkaListener(topics = "outbox-events-topic", groupId = "inventory-service-group")
    @Transactional
    public void consumeEvent(String message) {
        try {
            // 1. Validar que el mensaje no sea nulo
            if (message == null || message.trim().isEmpty()) {
                log.warn("⚠️ Mensaje nulo o vacío recibido. Ignorando.");
                return;
            }

            // 2. Parsear el envelope de Debezium
            JsonNode root = objectMapper.readTree(message);
            JsonNode after = root.get("after");

            // 3. Validar que exista el campo "after" (los DELETEs no lo tienen)
            if (after == null || after.isNull()) {
                log.warn("⚠️ Mensaje ignorado (operación DELETE o sin datos 'after')");
                return;
            }

            // 4. Validar que exista el campo "id"
            JsonNode idNode = after.get("id");
            if (idNode == null || idNode.isNull()) {
                log.warn("⚠️ Mensaje ignorado (no tiene campo 'id')");
                return;
            }

            // 5. Extraer el ID del evento Outbox (para idempotencia)
            String outboxEventIdStr = idNode.asText();
            UUID outboxEventId = UUID.fromString(outboxEventIdStr);

            // 6. VERIFICAR IDEMPOTENCIA: ¿Ya procesamos este evento?
            if (consumedEventRepository.existsByOutboxEventId(outboxEventId)) {
                log.info("ℹ️ Evento DUPLICADO detectado (ID: {}). Ignorando para evitar reprocesamiento.", outboxEventId);
                return;
            }

            // 7. Validar que exista el campo "payload"
            JsonNode payloadNode = after.get("payload");
            if (payloadNode == null || payloadNode.isNull()) {
                log.warn("⚠️ Mensaje ignorado (no tiene campo 'payload')");
                return;
            }

            // 8. Extraer los datos de negocio (el payload que guardamos en Spring Boot)
            String businessPayloadStr = payloadNode.asText();
            JsonNode businessPayload = objectMapper.readTree(businessPayloadStr);

            // 9. Validar campos del payload
            if (!businessPayload.has("pedidoId") || !businessPayload.has("cliente") || !businessPayload.has("total")) {
                log.warn("⚠️ Payload incompleto. Faltan campos requeridos.");
                return;
            }

            String pedidoId = businessPayload.get("pedidoId").asText();
            String cliente = businessPayload.get("cliente").asText();
            double total = businessPayload.get("total").asDouble();

            // 10. EJECUTAR LÓGICA DE NEGOCIO (Simulación de Inventario)
            log.info("📦 [INVENTARIO] Procesando pedido {} para cliente {}. Total: ${}", pedidoId, cliente, total);
            log.info("✅ [INVENTARIO] Stock reservado exitosamente para el pedido {}", pedidoId);

            // 11. GUARDAR REGISTRO DE IDEMPOTENCIA
            ConsumedEvent consumedEvent = ConsumedEvent.builder()
                    .outboxEventId(outboxEventId)
                    .aggregateId(UUID.fromString(pedidoId))
                    .processedAt(LocalDateTime.now())
                    .build();

            consumedEventRepository.save(consumedEvent);
            log.info("💾 Evento {} marcado como procesado en la base de datos.", outboxEventId);

        } catch (Exception e) {
            // 12. MANEJO DE ERRORES Y DLQ
            log.error("❌ Error fatal procesando el mensaje. Enviando a Dead Letter Queue (DLQ). Motivo: {}", e.getMessage());

            try {
                // Enviamos el mensaje original (el JSON de Debezium) a la DLQ para poder inspeccionarlo después
                kafkaTemplate.send(dlqTopicName, message);
                log.info("📩 Mensaje enviado exitosamente a la DLQ: {}", dlqTopicName);
            } catch (Exception ex) {
                log.error("💀 CRÍTICO: No se pudo enviar el mensaje a la DLQ. El mensaje se perderá. Error: {}", ex.getMessage());
            }
        }
    }*/
}