# Outbox Pattern + CDC + Saga Pattern + CQRS con Spring Boot, Debezium, Kafka y MongoDB

[![Java](https://img.shields.io/badge/Java-17+-blue.svg)](https://openjdk.java.net/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.2.x-brightgreen.svg)](https://spring.io/projects/spring-boot)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.7-red.svg)](https://kafka.apache.org/)
[![Debezium](https://img.shields.io/badge/Debezium-2.5-orange.svg)](https://debezium.io/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue.svg)](https://www.postgresql.org/)
[![MongoDB](https://img.shields.io/badge/MongoDB-7.0-green.svg)](https://www.mongodb.com/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## 📋 Propósito

Este proyecto implementa una **arquitectura de microservicios Event-Driven de nivel empresarial** que combina cuatro patrones fundamentales de sistemas distribuidos modernos. Su objetivo es demostrar cómo construir sistemas resilientes, desacoplados, tolerantes a fallos y altamente escalables, similares a los utilizados por empresas como Uber, Netflix o grandes instituciones financieras.

## 🎯 Problemas que Resuelve

1. **Dual Write Problem & Latencia:** ¿Cómo garantizar que un cambio en la base de datos y la publicación de un evento ocurran de forma atómica y en tiempo real?
    - **Solución:** Outbox Pattern + CDC con Debezium (lectura del WAL).
2. **Transacciones Distribuidas:** ¿Cómo manejar un proceso de negocio que involucra múltiples servicios (Pedidos → Inventario → Pagos) sin usar transacciones distribuidas costosas (2PC)?
    - **Solución:** Saga Pattern con transacciones de compensación.
3. **Cuellos de Botella en Lecturas/Escrituras:** ¿Cómo escalar un sistema donde el 90% del tráfico son lecturas sin afectar el rendimiento de las escrituras transaccionales?
    - **Solución:** CQRS con Persistencia Políglota (PostgreSQL + MongoDB).
4. **Tolerancia a Fallos:** ¿Qué pasa si un servicio falla, un mensaje se corrompe o Kafka reenvía duplicados?
    - **Solución:** Idempotencia + Dead Letter Queue (DLQ).

---

## 🏗️ Arquitectura
┌─────────────────────────────────────────────────────────────────────────┐
│ APLICACIÓN SPRING BOOT │
│ │
│ [REST API] ──▶ [PedidoService] ────▶ Tabla: pedidos (Write DB) │
│ ──▶ Tabla: outbox_events │
│ (Transacción Atómica ACID) │
└─────────────────────────────────────────────────────────────────────────┘
│
│ (WAL - Write-Ahead Log)
▼
┌─────────────────────────────────────────────────────────────────────────┐
│ POSTGRESQL 15 │
│ │
│ Base de datos transaccional (Write Side). Optimizada para consistencia.│
└─────────────────────────────────────────────────────────────────────────┘
│
│ (Replicación Lógica / CDC)
▼
┌─────────────────────────────────────────────────────────────────────────┐
│ DEBEZIUM (Kafka Connect) │
│ │
│ Lee el WAL en tiempo real (< 100ms latencia) y publica en Kafka. │
└─────────────────────────────────────────────────────────────────────────┘
│
▼
─────────────────────────────────────────────────────────────────────────
│ APACHE KAFKA │
│ │
│ Tópico Principal: outbox-events-topic │
│ Tópico DLQ: outbox-events-topic.DLQ (mensajes fallidos) │
└─────────────────────────────────────────────────────────────────────────┘
│ │
▼ ▼
┌──────────────────────────┐ ┌───────────────────────────────┐
│ SAGA PATTERN │ │ CQRS READ PROJECTOR │
│ (Coreografía) │ │ │
│ [Inventario] ─▶ [Pagos] │ │ Escucha eventos y actualiza │
│ │ Compensación │ │ la base de datos de lectura. │
──────────────────────────┘ └───────────────────────────────┘
│
▼
┌─────────────────────────────────────────────────────────────────────────┐
│ MONGODB 7.0 │
│ │
│ Base de datos de documentos (Read Side). Optimizada para consultas │
│ rápidas, desnormalización y escalabilidad horizontal. │
└─────────────────────────────────────────────────────────────────────────┘


---

## ✨ Características Implementadas

### 🔒 Consistencia y Confiabilidad

- **Transacciones Atómicas (ACID):** El pedido y el evento outbox se guardan juntos en la misma transacción SQL.
- **CDC en Tiempo Real:** Latencia < 100ms usando el WAL de PostgreSQL (sin consultas de polling).
- **Idempotencia:** Tabla `consumed_events` evita el procesamiento duplicado de mensajes.
- **Dead Letter Queue (DLQ):** Los mensajes que fallan al procesarse se mueven a un tópico de cuarentena para evitar bloquear el flujo principal.

### 🔄 Saga Pattern (Coreografía)

- **Flujo Distribuido:** Los servicios se comunican solo mediante eventos.
- **Happy Path:** Pedido Creado -> Inventario Reservado -> Pago Aprobado -> Pedido Confirmado.
- **Transacciones de Compensación:** Si el pago falla, se ejecuta automáticamente la liberación de inventario y el pedido se cancela.

### 📊 CQRS y Persistencia Políglota

- **Separación de Modelos:** Modelo de dominio complejo para escrituras (PostgreSQL), modelo plano y desnormalizado para lecturas (MongoDB).
- **Aislamiento de Recursos:** Las consultas masivas no afectan la base de datos transaccional.
- **Flexibilidad de Esquema:** MongoDB permite evolucionar la vista de lectura sin alterar la estructura relacional.

### 🛠️ Mantenimiento y Operaciones

- **Limpieza Automática:** Job programado borra eventos antiguos de `outbox_events` para evitar crecimiento infinito.
- **Gestión de Tópicos como Código:** Los tópicos de Kafka se crean automáticamente al iniciar la app mediante `TopicBuilder`.

---

## ⚙️ Tecnologías Utilizadas

| Componente | Tecnología | Versión | Rol en la Arquitectura |
|------------|------------|---------|------------------------|
| **Backend** | Java + Spring Boot | 17+ / 3.2.x | Lógica de negocio, API REST, Consumidores |
| **Write DB** | PostgreSQL | 15 | Base de datos transaccional (ACID) |
| **CDC** | Debezium | 2.5 | Captura de cambios leyendo el WAL |
| **Broker** | Apache Kafka | 3.7 (KRaft) | Distribución de eventos |
| **Read DB** | MongoDB | 7.0 | Base de datos de lectura (CQRS) |
| **Build Tool** | Gradle | 8.x | Gestión de dependencias y build |
| **Infraestructura** | Docker + Compose | Latest | Contenedorización de servicios |
| **Monitoreo** | Kafka UI | Latest | Interfaz gráfica para Kafka |

---

## 📦 Estructura del Proyecto
outbox-pattern/
├── src/main/java/com/ejemplo/outbox/
│ ├── config/
│ │ └── KafkaTopicConfig.java # Crea tópicos automáticamente
│ ├── consumer/
│ │ ├── SagaConsumers.java # Saga Pattern (Coreografía)
│ │ └── ReadModelProjector.java # Proyector CQRS (Read Side)
│ ├── controller/
│ │ ├── PedidoController.java # REST API (Write Side)
│ │ └── PedidoReadController.java # REST API (Read Side - CQRS)
│ ├── document/
│ │ └── PedidoReadDocument.java # Documento MongoDB (Read Model)
│ ├── entity/
│ │ ├── Pedido.java # Entidad de negocio (Write Model)
│ │ ├── OutboxEvent.java # Entidad del buzón
│ │ └── ConsumedEvent.java # Entidad para idempotencia
│ ├── repository/
│ │ ├── PedidoRepository.java # JPA Repository (PostgreSQL)
│ │ ├── OutboxEventRepository.java # Incluye query de limpieza
│ │ ├── ConsumedEventRepository.java # Para idempotencia
│ │ ── PedidoReadRepository.java # Mongo Repository
│ ├── service/
│ │ ├── PedidoService.java # Transacción atómica + Outbox
│ │ └── OutboxCleaner.java # Job programado de limpieza
│ └── OutboxPatternRelayApplication.java
├── src/main/resources/
│ └── application.properties # Configuración de la app
├── debezium-connector.json # Configuración del conector CDC
├── docker-compose.yml # Infraestructura completa
└── build.gradle # Dependencias del proyecto


---

## 🚀 Cómo Ejecutar

### Prerrequisitos

- Docker y Docker Compose instalados
- Java 17 o superior
- Gradle (o usar el wrapper `gradlew`)

### Paso 1: Levantar la Infraestructura

```bash
# Iniciar PostgreSQL, Kafka, Kafka Connect, Kafka UI y MongoDB
docker-compose up -d

# Verificar que todos los servicios estén corriendo
docker-compose ps

Servicios disponibles:
PostgreSQL: localhost:5432 (Usuario: admin, Contraseña: admin123, DB: outbox_db)
Kafka Broker: localhost:29092
Kafka Connect (Debezium): localhost:8083
Kafka UI: http://localhost:8080
MongoDB: localhost:27017 (Usuario: admin, Contraseña: admin123, DB: read_db)
Paso 2: Registrar el Conector Debezium
Una vez que los contenedores estén listos (esperar 30-40 segundos), registra el conector:
curl -i -X POST -H "Accept:application/json" \
  -H "Content-Type:application/json" \
  http://localhost:8083/connectors \
  -d @debezium-connector.json

Verificar estado del conector:
curl http://localhost:8083/connectors/outbox-connector/status
Debe mostrar "state": "RUNNING".

Paso 3: Ejecutar la Aplicación Spring Boot
# Linux/Mac
./gradlew bootRun

# Windows
gradlew.bat bootRun
La API REST estará disponible en: http://localhost:8081

🧪 Pruebas y Validación
1. Crear un Pedido (Write Side)
curl -X POST http://localhost:8081/api/pedidos \
-H "Content-Type: application/json" \
-d '{
  "cliente": "Juan Pérez",
  "total": 350.50
}'
Respuesta esperada:
{
  "id": "uuid-del-pedido",
  "cliente": "Juan Pérez",
  "total": 350.50,
  "fechaCreacion": "2026-06-18T10:00:00",
  "estado": "PENDIENTE_PAGO"
}

2. Observar el Flujo del Saga
En los logs de Spring Boot verás la coreografía en tiempo real:
Caso de Éxito (70% de probabilidad):
📦 [INVENTARIO] Reservando stock para pedido: <uuid>
💳 [PAGOS] Procesando pago para pedido: <uuid>
✅ [PAGOS] Pago aprobado para pedido: <uuid>
🎉 [PEDIDOS] Pedido <uuid> CONFIRMADO exitosamente.

Caso de Compensación (30% de probabilidad):
📦 [INVENTARIO] Reservando stock para pedido: <uuid>
💳 [PAGOS] Procesando pago para pedido: <uuid>
❌ [PAGOS] Pago RECHAZADO para pedido: <uuid>. Iniciando compensación...
🔄 [INVENTARIO-COMP] Liberando stock para pedido cancelado: <uuid>
🛑 [PEDIDOS-COMP] Pedido <uuid> CANCELADO y stock liberado.

3. Consultar el Read Model (CQRS - MongoDB)
# Obtener todos los pedidos desde MongoDB (lectura rápida)
curl http://localhost:8081/api/consultas/pedidos

# Filtrar pedidos por estado
curl http://localhost:8081/api/consultas/pedidos/filtrar?estado=CONFIRMADO

4. Verificar en la Base de Datos
PostgreSQL (Write Side):
SELECT id, cliente, estado, created_at 
FROM pedidos 
ORDER BY created_at DESC 
LIMIT 10;

MongoDB (Read Side):
# Entrar al contenedor de MongoDB
docker exec -it outbox-mongo mongosh -u admin -p admin123 --authenticationDatabase admin

# Seleccionar base de datos y consultar
use read_db
db.pedidos_vista_rapida.find().pretty()

5. Verificar en Kafka UI
Abre http://localhost:8080 y explora:
Topics → outbox-events-topic: Verás todos los eventos del Saga.
Topics → outbox-events-topic.DLQ: Verás mensajes fallidos (si los hay).
🔄 Flujo del Saga Pattern
Happy Path (Todo sale bien)
Cliente → POST /api/pedidos
PedidoService → Guarda pedido (PENDIENTE_PAGO) + evento PEDIDO_CREADO
Debezium → Detecta cambio en WAL → Publica en Kafka
Inventario → Escucha PEDIDO_CREADO → Reserva stock → Emite INVENTARIO_RESERVADO
Pagos → Escucha INVENTARIO_RESERVADO → Cobra tarjeta → Emite PAGO_APROBADO
Pedidos → Escucha PAGO_APROBADO → Actualiza estado a CONFIRMADO
ReadModelProjector → Escucha PAGO_APROBADO → Actualiza MongoDB
Failure Path (Pago rechazado)
Cliente → POST /api/pedidos
PedidoService → Guarda pedido (PENDIENTE_PAGO) + evento PEDIDO_CREADO
Debezium → Detecta cambio en WAL → Publica en Kafka
Inventario → Escucha PEDIDO_CREADO → Reserva stock → Emite INVENTARIO_RESERVADO
Pagos → Escucha INVENTARIO_RESERVADO → ❌ Pago rechazado → Emite PAGO_RECHAZADO
Inventario (Compensación) → Escucha PAGO_RECHAZADO → Libera stock → Emite INVENTARIO_LIBERADO
Pedidos (Compensación) → Escucha INVENTARIO_LIBERADO → Actualiza estado a CANCELADO
ReadModelProjector → Escucha PAGO_RECHAZADO → Actualiza MongoDB a CANCELADO
Comparación: Polling vs CDC

⚙️ Configuración Importante
application.properties
# Puerto de la aplicación
server.port=8081

# Conexión a PostgreSQL (Write Side)
spring.datasource.url=jdbc:postgresql://localhost:5432/outbox_db
spring.datasource.username=admin
spring.datasource.password=admin123
spring.jpa.hibernate.ddl-auto=update

# Conexión a MongoDB (Read Side - CQRS)
spring.data.mongodb.uri=mongodb://admin:admin123@localhost:27017/read_db?authSource=admin

# Conexión a Kafka
spring.kafka.bootstrap-servers=localhost:29092
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer

# Tópicos de Kafka
kafka.topic.outbox=outbox-events-topic
kafka.topic.dlq=outbox-events-topic.DLQ

# Configuración de consumidores
spring.kafka.consumer.auto-offset-reset=latest
spring.kafka.consumer.group-id=outbox-consumer-group

docker-compose.yml (PostgreSQL con CDC)
postgres:
  image: postgres:15-alpine
  command: 
    - postgres
    - -c
    - wal_level=logical
    - -c
    - max_wal_senders=10
    - -c
    - max_replication_slots=10

️ Comandos Útiles
Ver conectores Debezium registrados:
curl http://localhost:8083/connectors

Ver estado del conector:
curl http://localhost:8083/connectors/outbox-connector/status

Ver logs de Debezium en tiempo real:
docker-compose logs -f connect

Reiniciar el conector:
curl -X POST http://localhost:8083/connectors/outbox-connector/restart

Limpiar base de datos para pruebas:
-- PostgreSQL
TRUNCATE TABLE consumed_events;
TRUNCATE TABLE outbox_events;
TRUNCATE TABLE pedidos;

# MongoDB
docker exec -it outbox-mongo mongosh -u admin -p admin123 --authenticationDatabase admin
use read_db
db.pedidos_vista_rapida.deleteMany({})

🎓 Conceptos Clave Implementados
Event-Driven Architecture (EDA)
Arquitectura donde los servicios se comunican mediante eventos asíncronos, logrando desacoplamiento total.
Outbox Pattern
Patrón que garantiza la entrega confiable de eventos guardándolos en una tabla intermedia dentro de la misma transacción que el dato de negocio.
Change Data Capture (CDC)
Técnica que captura cambios en la base de datos leyendo el log de transacciones (WAL), permitiendo replicación en tiempo real sin afectar el rendimiento.
Saga Pattern
Patrón para manejar transacciones distribuidas mediante una secuencia de transacciones locales con compensación automática ante fallos.
Idempotencia
Garantía de que procesar el mismo evento múltiples veces tiene el mismo efecto que procesarlo una sola vez.
Dead Letter Queue (DLQ)
Cola de cuarentena donde se mueven los mensajes que fallan al procesarse, evitando bloquear el flujo principal.
CQRS (Command Query Responsibility Segregation)
Patrón que separa las operaciones de lectura y escritura en modelos diferentes, permitiendo optimizar cada uno de forma independiente.
Persistencia Políglota
Uso de diferentes tecnologías de base de datos según las necesidades específicas de cada componente del sistema.
📚 Recursos Adicionales
Debezium Documentation
Transactional Outbox Pattern
Saga Pattern
Apache Kafka Documentation
Event-Driven Architecture
CQRS Pattern
MongoDB Documentation
🤝 Contribución
Este proyecto es un ejemplo educativo de arquitecturas de microservicios resilientes. Si encuentras bugs o mejoras, ¡las contribuciones son bienvenidas!
Licencia
MIT License - Siente libre de usar este código en tus proyectos.
Desarrollado con ❤️ como ejemplo educativo de patrones avanzados en arquitecturas de microservicios.
Este proyecto demuestra la implementación práctica de Outbox Pattern, CDC con Debezium, Saga Pattern por Coreografía y CQRS con Persistencia Políglota, proporcionando una base sólida para construir sistemas distribuidos resilientes y escalables.