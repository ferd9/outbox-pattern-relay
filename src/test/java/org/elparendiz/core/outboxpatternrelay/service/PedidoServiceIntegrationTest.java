package org.elparendiz.core.outboxpatternrelay.service;



import org.elparendiz.core.outboxpatternrelay.AbstractIntegrationTest;
import org.elparendiz.core.outboxpatternrelay.entity.Pedido;
import org.elparendiz.core.outboxpatternrelay.repository.PedidoRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
class PedidoServiceIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private PedidoService pedidoService;

    @Autowired
    private PedidoRepository pedidoRepository;

    @Test
    @DisplayName("Debe crear un pedido con estado PENDIENTE_PAGO")
    void shouldCrearPedidoConEstadoPendiente() {
        // Given
        String cliente = "Juan Test";
        BigDecimal total = new BigDecimal("150.00");

        // When
        Pedido pedido = pedidoService.crearPedido(cliente, total);

        // Then
        assertThat(pedido).isNotNull();
        assertThat(pedido.getId()).isNotNull();
        assertThat(pedido.getCliente()).isEqualTo(cliente);
        assertThat(pedido.getTotal()).isEqualByComparingTo(total);
        assertThat(pedido.getEstado()).isEqualTo(Pedido.EstadoPedido.PENDIENTE_PAGO);

        // Verificar que se guardó en la base de datos
        assertThat(pedidoRepository.findById(pedido.getId())).isPresent();
    }

    @Test
    @DisplayName("Debe actualizar el estado del pedido")
    void shouldActualizarEstadoPedido() {
        // Given
        Pedido pedido = pedidoService.crearPedido("Cliente Test", new BigDecimal("100.00"));

        // When
        pedido.setEstado(Pedido.EstadoPedido.CONFIRMADO);
        Pedido updated = pedidoRepository.save(pedido);

        // Then
        assertThat(updated.getEstado()).isEqualTo(Pedido.EstadoPedido.CONFIRMADO);
    }
}