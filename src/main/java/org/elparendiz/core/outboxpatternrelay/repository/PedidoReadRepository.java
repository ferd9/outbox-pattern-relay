package org.elparendiz.core.outboxpatternrelay.repository;


import org.elparendiz.core.outboxpatternrelay.document.PedidoReadDocument;
import org.springframework.data.mongodb.repository.MongoRepository;
import java.util.List;

public interface PedidoReadRepository extends MongoRepository<PedidoReadDocument, String> {
    // Ejemplo de consulta rápida y flexible que en SQL sería compleja
    List<PedidoReadDocument> findByEstado(String estado);
}
