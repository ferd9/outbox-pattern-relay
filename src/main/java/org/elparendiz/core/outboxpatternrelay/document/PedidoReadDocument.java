package org.elparendiz.core.outboxpatternrelay.document;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Document(collection = "pedidos_vista_rapida") // Nombre de la colección en MongoDB
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PedidoReadDocument {

    @Id
    private String id; // MongoDB usa String para los IDs por defecto

    private String cliente;
    private BigDecimal total;
    private String estado;
    private LocalDateTime fechaCreacion;
}
