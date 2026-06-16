package org.elparendiz.core.outboxpatternrelay.repository;


import org.elparendiz.core.outboxpatternrelay.entity.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {
    // Buscamos solo los eventos que están pendientes de ser publicados
    List<OutboxEvent> findByEstado(OutboxEvent.EstadoOutbox estado);

    // NUEVO: Consulta para borrar eventos antiguos
    @Modifying(clearAutomatically = true) // clearAutomatically evita problemas de caché en Hibernate
    @Query("DELETE FROM OutboxEvent o WHERE o.createdAt < :threshold")
    int deleteOlderThan(@Param("threshold") LocalDateTime threshold);
}