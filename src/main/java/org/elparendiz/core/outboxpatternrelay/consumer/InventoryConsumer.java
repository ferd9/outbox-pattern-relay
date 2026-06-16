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
    private final KafkaTemplate<String, String> kafkaTemplate; // Inyectamos el template

    @Value("${kafka.topic.dlq}")
    private String dlqTopicName;

    @KafkaListener(topics = "outbox-events-topic", groupId = "inventory-service-group")
    @Transactional
    public void consumeEvent(String message) {
        try {
            // 1. Parsear el envelope de Debezium
            JsonNode root = objectMapper.readTree(message);
            JsonNode after = root.get("after");

            if (after == null) {
                log.warn(" Mensaje ignorado (operación DELETE o sin datos 'after')");
                return;
            }

            // 2. Extraer el ID del evento Outbox (para idempotencia)
            String outboxEventIdStr = after.get("id").asText();
            UUID outboxEventId = UUID.fromString(outboxEventIdStr);

            // 3. VERIFICAR IDEMPOTENCIA
            if (consumedEventRepository.existsByOutboxEventId(outboxEventId)) {
                log.info(" Evento DUPLICADO detectado (ID: {}). Ignorando.", outboxEventId);
                return;
            }

            // 4. Extraer los datos de negocio
            String businessPayloadStr = after.get("payload").asText();
            JsonNode businessPayload = objectMapper.readTree(businessPayloadStr);

            String pedidoId = businessPayload.get("pedidoId").asText();
            String cliente = businessPayload.get("cliente").asText();
            double total = businessPayload.get("total").asDouble();

            // 5. LÓGICA DE NEGOCIO (Simulación)
            log.info(" [INVENTARIO] Procesando pedido {} para cliente {}. Total: ${}", pedidoId, cliente, total);

            // Simulemos un error aleatorio para probar la DLQ (Descomenta la siguiente línea para probar)
            //if (Math.random() > 0.5) throw new RuntimeException("Error simulado de inventario");

            log.info(" [INVENTARIO] Stock reservado exitosamente para el pedido {}", pedidoId);

            // 6. GUARDAR REGISTRO DE IDEMPOTENCIA
            ConsumedEvent consumedEvent = ConsumedEvent.builder()
                    .outboxEventId(outboxEventId)
                    .aggregateId(UUID.fromString(pedidoId))
                    .processedAt(LocalDateTime.now())
                    .build();

            consumedEventRepository.save(consumedEvent);

        } catch (Exception e) {
            // 7. MANEJO DE ERRORES Y DLQ
            log.error(" Error fatal procesando el mensaje. Enviando a Dead Letter Queue (DLQ). Motivo: {}", e.getMessage());

            try {
                // Enviamos el mensaje original (el JSON de Debezium) a la DLQ para poder inspeccionarlo después
                kafkaTemplate.send(dlqTopicName, message);
                log.info(" Mensaje enviado exitosamente a la DLQ: {}", dlqTopicName);
            } catch (Exception ex) {
                log.error(" CRÍTICO: No se pudo enviar el mensaje a la DLQ. El mensaje se perderá. Error: {}", ex.getMessage());
            }
        }
    }
}
