package org.elparendiz.core.outboxpatternrelay.repository;


import org.elparendiz.core.outboxpatternrelay.entity.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import java.util.UUID;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {
    // Buscamos solo los eventos que están pendientes de ser publicados
    List<OutboxEvent> findByEstado(OutboxEvent.EstadoOutbox estado);
}