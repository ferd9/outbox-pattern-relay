package org.elparendiz.core.outboxpatternrelay;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
class OutboxPatternRelayApplicationTests extends AbstractIntegrationTest {

    @Test
    void contextLoads() {
        // Este test verifica que el contexto de Spring se carga correctamente
        // con todos los contenedores de Testcontainers (PostgreSQL, MongoDB, Kafka)
        assertTrue(true, "El contexto de Spring se cargó exitosamente con Testcontainers");
    }
}