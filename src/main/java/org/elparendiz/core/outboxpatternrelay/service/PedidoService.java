package org.elparendiz.core.outboxpatternrelay.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elparendiz.core.outboxpatternrelay.entity.OutboxEvent;
import org.elparendiz.core.outboxpatternrelay.entity.Pedido;
import org.elparendiz.core.outboxpatternrelay.repository.OutboxEventRepository;
import org.elparendiz.core.outboxpatternrelay.repository.PedidoRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class PedidoService {

    private final PedidoRepository pedidoRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper; // Para convertir el objeto a JSON String

    @Transactional // ¡CLAVE! Garantiza que ambas inserciones sean atómicas
    public Pedido crearPedido(String cliente, BigDecimal total) {
        log.info("Iniciando creación de pedido para: {}", cliente);

        // 1. Crear y guardar el dato de negocio
        Pedido pedido = Pedido.builder()
                .cliente(cliente)
                .total(total)
                .fechaCreacion(LocalDateTime.now())
                .build();
        pedido = pedidoRepository.save(pedido);

        // 2. Crear el evento para el Outbox
        Map<String, Object> payloadData = new HashMap<>();
        payloadData.put("pedidoId", pedido.getId().toString());
        payloadData.put("cliente", pedido.getCliente());
        payloadData.put("total", pedido.getTotal());

        try {
            OutboxEvent evento = OutboxEvent.builder()
                    .aggregateId(pedido.getId()) // Usaremos esto como KEY en Kafka
                    .eventType("PEDIDO_CREADO")
                    .payload(objectMapper.writeValueAsString(payloadData)) // Serializamos a JSON
                    .estado(OutboxEvent.EstadoOutbox.PENDING)
                    .createdAt(LocalDateTime.now())
                    .reintentos(0)
                    .build();

            // 3. Guardar el evento en la MISMA transacción
            outboxEventRepository.save(evento);
            log.info("Pedido y Evento Outbox guardados exitosamente en la BD.");

        } catch (Exception e) {
            log.error("Error al serializar el payload del evento", e);
            throw new RuntimeException("Fallo al preparar el evento Outbox", e);
        }

        return pedido;
    }
}