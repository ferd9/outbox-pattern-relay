package org.elparendiz.core.outboxpatternrelay.consumer;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elparendiz.core.outboxpatternrelay.entity.ConsumedEvent;
import org.elparendiz.core.outboxpatternrelay.entity.Pedido;
import org.elparendiz.core.outboxpatternrelay.repository.ConsumedEventRepository;
import org.elparendiz.core.outboxpatternrelay.repository.PedidoRepository;
import org.elparendiz.core.outboxpatternrelay.service.PedidoService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class SagaConsumers {

    private final PedidoRepository pedidoRepository;
    private final ConsumedEventRepository consumedEventRepository;
    private final PedidoService pedidoService;
    private final ObjectMapper objectMapper;

    // ==========================================
    // 1. SERVICIO DE INVENTARIO (Happy Path)
    // ==========================================
    @KafkaListener(topics = "outbox-events-topic", groupId = "inventory-saga-group")
    @Transactional
    public void procesarPedidoCreado(String message) {
        try {
            if (!esEventoValido(message, "PEDIDO_CREADO")) return;

            JsonNode payload = obtenerPayload(message);
            if (payload == null || !payload.has("pedidoId")) return;

            UUID pedidoId = UUID.fromString(payload.get("pedidoId").asText());

            log.info("📦 [INVENTARIO] Reservando stock para pedido: {}", pedidoId);

            Map<String, Object> data = new HashMap<>();
            data.put("pedidoId", pedidoId.toString());
            pedidoService.publicarEventoSaga("INVENTARIO", pedidoId, "INVENTARIO_RESERVADO", data);
        } catch (Exception e) {
            log.error("❌ Error en procesarPedidoCreado: {}", e.getMessage());
        }
    }

    // ==========================================
    // 2. SERVICIO DE PAGOS (Simulación con fallo)
    // ==========================================
    @KafkaListener(topics = "outbox-events-topic", groupId = "payment-saga-group")
    @Transactional
    public void procesarInventarioReservado(String message) {
        try {
            if (!esEventoValido(message, "INVENTARIO_RESERVADO")) return;

            JsonNode payload = obtenerPayload(message);
            if (payload == null || !payload.has("pedidoId")) return;

            UUID pedidoId = UUID.fromString(payload.get("pedidoId").asText());
            log.info("💳 [PAGOS] Procesando pago para pedido: {}", pedidoId);

            // SIMULACIÓN: 70% de éxito, 30% de fallo
            boolean pagoExitoso = Math.random() > 0.3;

            Map<String, Object> data = new HashMap<>();
            data.put("pedidoId", pedidoId.toString());

            if (pagoExitoso) {
                log.info("✅ [PAGOS] Pago aprobado para pedido: {}", pedidoId);
                pedidoService.publicarEventoSaga("PAGO", pedidoId, "PAGO_APROBADO", data);
            } else {
                log.error("❌ [PAGOS] Pago RECHAZADO para pedido: {}. Iniciando compensación...", pedidoId);
                pedidoService.publicarEventoSaga("PAGO", pedidoId, "PAGO_RECHAZADO", data);
            }
        } catch (Exception e) {
            log.error("❌ Error en procesarInventarioReservado: {}", e.getMessage());
        }
    }

    // ==========================================
    // 3. SERVICIO DE PEDIDOS (Finalización)
    // ==========================================
    @KafkaListener(topics = "outbox-events-topic", groupId = "order-saga-group")
    @Transactional
    public void procesarPagoAprobado(String message) {
        try {
            if (!esEventoValido(message, "PAGO_APROBADO")) return;

            JsonNode payload = obtenerPayload(message);
            if (payload == null || !payload.has("pedidoId")) return;

            UUID pedidoId = UUID.fromString(payload.get("pedidoId").asText());
            actualizarEstadoPedido(pedidoId, Pedido.EstadoPedido.CONFIRMADO);
            log.info("🎉 [PEDIDOS] Pedido {} CONFIRMADO exitosamente.", pedidoId);
        } catch (Exception e) {
            log.error("❌ Error en procesarPagoAprobado: {}", e.getMessage());
        }
    }

    // ==========================================
    // 4. COMPENSACIÓN: Inventario (Si el pago falla)
    // ==========================================
    @KafkaListener(topics = "outbox-events-topic", groupId = "inventory-compensation-group")
    @Transactional
    public void procesarPagoRechazado(String message) {
        try {
            if (!esEventoValido(message, "PAGO_RECHAZADO")) return;

            JsonNode payload = obtenerPayload(message);
            if (payload == null || !payload.has("pedidoId")) return;

            UUID pedidoId = UUID.fromString(payload.get("pedidoId").asText());
            log.info("🔄 [INVENTARIO-COMP] Liberando stock para pedido cancelado: {}", pedidoId);

            Map<String, Object> data = new HashMap<>();
            data.put("pedidoId", pedidoId.toString());
            pedidoService.publicarEventoSaga("INVENTARIO", pedidoId, "INVENTARIO_LIBERADO", data);
        } catch (Exception e) {
            log.error("❌ Error en procesarPagoRechazado: {}", e.getMessage());
        }
    }

    // ==========================================
    // 5. COMPENSACIÓN: Pedidos (Finalizar cancelación)
    // ==========================================
    @KafkaListener(topics = "outbox-events-topic", groupId = "order-compensation-group")
    @Transactional
    public void procesarInventarioLiberado(String message) {
        try {
            if (!esEventoValido(message, "INVENTARIO_LIBERADO")) return;

            JsonNode payload = obtenerPayload(message);
            if (payload == null || !payload.has("pedidoId")) return;

            UUID pedidoId = UUID.fromString(payload.get("pedidoId").asText());
            actualizarEstadoPedido(pedidoId, Pedido.EstadoPedido.CANCELADO);
            log.info(" [PEDIDOS-COMP] Pedido {} CANCELADO y stock liberado.", pedidoId);
        } catch (Exception e) {
            log.error("❌ Error en procesarInventarioLiberado: {}", e.getMessage());
        }
    }

    // ==========================================
    // UTILIDADES SEGRAS
    // ==========================================
    private boolean esEventoValido(String message, String eventTypeEsperado) {
        if (message == null) return false;

        try {
            JsonNode root = objectMapper.readTree(message);
            JsonNode after = root.get("after");
            if (after == null) return false;

            // Verificar idempotencia
            JsonNode idNode = after.get("id");
            if (idNode == null || idNode.isNull()) return false;

            UUID outboxId = UUID.fromString(idNode.asText());
            if (consumedEventRepository.existsByOutboxEventId(outboxId)) return false;

            // Verificar tipo de evento (Debezium usa snake_case por defecto: event_type)
            JsonNode tipoNode = after.get("event_type");
            if (tipoNode == null) return false;

            String tipoEvento = tipoNode.asText();
            if (!tipoEvento.equals(eventTypeEsperado)) return false;

            // Marcar como consumido
            consumedEventRepository.save(ConsumedEvent.builder()
                    .outboxEventId(outboxId)
                    .processedAt(LocalDateTime.now())
                    .build());
            return true;
        } catch (Exception e) {
            log.warn("⚠️ Evento no válido o error de parsing: {}", e.getMessage());
            return false;
        }
    }

    private JsonNode obtenerPayload(String message) {
        try {
            JsonNode after = objectMapper.readTree(message).get("after");
            if (after == null) return null;

            JsonNode payloadNode = after.get("payload");
            if (payloadNode == null || payloadNode.isNull()) return null;

            return objectMapper.readTree(payloadNode.asText());
        } catch (Exception e) {
            log.error("❌ Error al obtener payload: {}", e.getMessage());
            return null;
        }
    }

    private void actualizarEstadoPedido(UUID pedidoId, Pedido.EstadoPedido estado) {
        pedidoRepository.findById(pedidoId).ifPresent(pedido -> {
            pedido.setEstado(estado);
            pedidoRepository.save(pedido);
        });
    }
}