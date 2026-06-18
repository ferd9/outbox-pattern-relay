package org.elparendiz.core.outboxpatternrelay.controller;

import lombok.RequiredArgsConstructor;
import org.elparendiz.core.outboxpatternrelay.document.PedidoReadDocument;
import org.elparendiz.core.outboxpatternrelay.repository.PedidoReadRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/consultas")
@RequiredArgsConstructor
public class PedidoReadController {

    private final PedidoReadRepository readRepository; // Repo de Mongo

    // Consulta general (Ultra rápida)
    @GetMapping("/pedidos")
    public ResponseEntity<List<PedidoReadDocument>> obtenerPedidosVistaRapida() {
        return ResponseEntity.ok(readRepository.findAll());
    }

    // Ejemplo de consulta específica que en SQL requeriría un WHERE
    @GetMapping("/pedidos/filtrar")
    public ResponseEntity<List<PedidoReadDocument>> obtenerPedidosPorEstado(
            @RequestParam String estado) {
        return ResponseEntity.ok(readRepository.findByEstado(estado));
    }
}
