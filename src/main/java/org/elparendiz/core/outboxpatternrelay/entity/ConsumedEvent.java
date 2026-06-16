package org.elparendiz.core.outboxpatternrelay.entity;

import jakarta.persistence.*;
import lombok.*;
import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "consumed_events")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ConsumedEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    // El ID único del evento de la tabla outbox_events (viene en el JSON de Debezium)
    @Column(name = "outbox_event_id", unique = true)
    private UUID outboxEventId;

    // El ID del pedido (para trazabilidad)
    @Column(name = "aggregate_id")
    private UUID aggregateId;

    private LocalDateTime processedAt;
}