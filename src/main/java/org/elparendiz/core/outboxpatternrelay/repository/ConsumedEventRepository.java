package org.elparendiz.core.outboxpatternrelay.repository;

import org.elparendiz.core.outboxpatternrelay.entity.ConsumedEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.UUID;

public interface ConsumedEventRepository extends JpaRepository<ConsumedEvent, UUID> {
    boolean existsByOutboxEventId(UUID outboxEventId);
}