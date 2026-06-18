package org.elparendiz.core.outboxpatternrelay.consumer;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elparendiz.core.outboxpatternrelay.document.PedidoReadDocument;
import org.elparendiz.core.outboxpatternrelay.repository.PedidoReadRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.LocalDateTime;


@Component
@RequiredArgsConstructor
@Slf4j
public class ReadModelProjector {

    private final PedidoReadRepository readRepository; // Inyectamos el repo de Mongo
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "outbox-events-topic", groupId = "read-model-projector-group")
    public void projectEvent(String message) {
        try {
            JsonNode root = objectMapper.readTree(message);
            JsonNode after = root.get("after");
            if (after == null) return;

            String eventType = after.get("event_type").asText();
            JsonNode payloadNode = after.get("payload");
            if (payloadNode == null || payloadNode.isNull()) return;

            JsonNode payload = objectMapper.readTree(payloadNode.asText());
            String pedidoId = payload.get("pedidoId").asText();

            switch (eventType) {
                case "PEDIDO_CREADO":
                    crearOActualizarReadModel(pedidoId, payload, "PENDIENTE_PAGO");
                    log.info(" [PROYECTOR-MONGO] Creado en Read DB: {}", pedidoId);
                    break;

                case "PAGO_APROBADO":
                    actualizarEstado(pedidoId, "CONFIRMADO");
                    log.info("📊 [PROYECTOR-MONGO] Actualizado a CONFIRMADO: {}", pedidoId);
                    break;

                case "PAGO_RECHAZADO":
                    actualizarEstado(pedidoId, "CANCELADO");
                    log.info("📊 [PROYECTOR-MONGO] Actualizado a CANCELADO: {}", pedidoId);
                    break;
            }
        } catch (Exception e) {
            log.error("❌ Error en el Proyector CQRS (Mongo): {}", e.getMessage());
        }
    }

    private void crearOActualizarReadModel(String id, JsonNode payload, String estado) {
        PedidoReadDocument doc = PedidoReadDocument.builder()
                .id(id)
                .cliente(payload.get("cliente").asText())
                .total(new BigDecimal(payload.get("total").asText()))
                .estado(estado)
                .fechaCreacion(LocalDateTime.now())
                .build();
        readRepository.save(doc); // Guardado en MongoDB
    }

    private void actualizarEstado(String id, String nuevoEstado) {
        readRepository.findById(id).ifPresent(doc -> {
            doc.setEstado(nuevoEstado);
            readRepository.save(doc); // Actualización en MongoDB
        });
    }
}
