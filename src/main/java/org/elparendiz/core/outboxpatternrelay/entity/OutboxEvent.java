package org.elparendiz.core.outboxpatternrelay.entity;

import jakarta.persistence.*;
import lombok.*;
import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "outbox_events")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OutboxEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    // ID del pedido (aggregate_id). Lo usaremos como KEY en Kafka para garantizar orden.
    @Column(name = "aggregate_id")
    private UUID aggregateId;

    @Column(name = "event_type")
    private String eventType; // Ej: "PedidoCreado"

    @Column(columnDefinition = "TEXT")
    private String payload; // El cuerpo del mensaje en formato JSON

    @Enumerated(EnumType.STRING)
    private EstadoOutbox estado; // PENDING, PROCESSED, FAILED

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    private Integer reintentos;

    // Enum para los estados
    public enum EstadoOutbox {
        PENDING,
        PROCESSED,
        FAILED
    }
}