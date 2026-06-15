package org.elparendiz.core.outboxpatternrelay.repository;

import org.elparendiz.core.outboxpatternrelay.entity.Pedido;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.UUID;

public interface PedidoRepository extends JpaRepository<Pedido, UUID> {
}
