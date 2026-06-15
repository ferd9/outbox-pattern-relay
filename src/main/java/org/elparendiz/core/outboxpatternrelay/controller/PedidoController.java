package org.elparendiz.core.outboxpatternrelay.controller;

import lombok.RequiredArgsConstructor;
import org.elparendiz.core.outboxpatternrelay.entity.Pedido;
import org.elparendiz.core.outboxpatternrelay.service.PedidoService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.Map;

@RestController
@RequestMapping("/api/pedidos")
@RequiredArgsConstructor
public class PedidoController {

    private final PedidoService pedidoService;

    @PostMapping
    public ResponseEntity<Pedido> crearPedido(@RequestBody Map<String, Object> request) {
        String cliente = (String) request.get("cliente");
        BigDecimal total = new BigDecimal(request.get("total").toString());

        // Llamamos al servicio transaccional
        Pedido nuevoPedido = pedidoService.crearPedido(cliente, total);

        // Respondemos inmediatamente al cliente.
        // ¡El evento de Kafka aún NO se ha publicado, pero está seguro en la BD!
        return ResponseEntity.ok(nuevoPedido);
    }
}