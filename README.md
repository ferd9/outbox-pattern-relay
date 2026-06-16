# Outbox Pattern con CDC (Debezium) y Apache Kafka

[![Java](https://img.shields.io/badge/Java-17+-blue.svg)](https://openjdk.java.net/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.2.x-brightgreen.svg)](https://spring.io/projects/spring-boot)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.7-red.svg)](https://kafka.apache.org/)
[![Debezium](https://img.shields.io/badge/Debezium-2.5-orange.svg)](https://debezium.io/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue.svg)](https://www.postgresql.org/)

## 📋 Propósito

Este proyecto implementa una arquitectura de microservicios robusta basada en el **Outbox Pattern** combinado con **CDC (Change Data Capture)** utilizando Debezium.

El objetivo es garantizar la **entrega confiable de eventos** en sistemas distribuidos, resolviendo el problema de la doble escritura (Dual Write Problem), asegurando la consistencia eventual, y gestionando fallos mediante colas de cartas muertas (DLQ) e idempotencia.

##  Problema que Resuelve

En arquitecturas basadas en eventos, publicar mensajes directamente desde el servicio puede causar pérdida de datos si el broker de mensajes falla.
1. **Outbox Pattern**: Guarda el evento en la base de datos en la misma transacción que el dato de negocio.
2. **CDC con Debezium**: En lugar de consultar la base de datos constantemente (Polling), Debezium lee el **WAL (Write-Ahead Log)** de PostgreSQL en tiempo real (< 100ms de latencia) y publica los cambios en Kafka.

## 🏗️ Arquitectura

```text
─────────────────────────────────────────────────────────────────────────┐
│                         APLICACIÓN SPRING BOOT                          │
│                                                                         │
│  [REST API] ──▶ [PedidoService] ──┬──▶ Tabla: pedidos (Datos internos) │
│                                   └──▶ Tabla: outbox_events (Buzón)     │
─────────────────────────────────────────────────────────────────────────
                                          │
                                          │ (Transacción Atómica)
                                          ▼
─────────────────────────────────────────────────────────────────────────
│                              POSTGRESQL                                 │
│                                                                         │
│  Escribe los cambios en el WAL (Write-Ahead Log) en tiempo real.        │
└─────────────────────────────────────────────────────────────────────────┘
                                          │
                                          │ (Replicación Lógica / CDC)
                                          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                            DEBEZIUM (Kafka Connect)                     │
│                                                                         │
│  Lee el WAL, filtra la tabla 'outbox_events' y publica en Kafka.        │
└─────────────────────────────────────────────────────────────────────────┘
                                          │
                                          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                               APACHE KAFKA                              │
│                                                                         │
│  Tópico Principal: 'outbox-events-topic'                                │
│  Tópico DLQ: 'outbox-events-topic.DLQ' (Para mensajes fallidos)         │
└─────────────────────────────────────────────────────────────────────────┘
                                          │
                                          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         SERVICIO CONSUMIDOR                             │
│                                                                         │
│  [InventoryConsumer] ─▶ Verifica Idempotencia (Tabla: consumed_events) │
│                        ──▶ Ejecuta Lógica de Negocio                    │
│                        ──▶ Si falla ──▶ Envía a Dead Letter Queue (DLQ) │
└─────────────────────────────────────────────────────────────────────────┘

✨ Características Clave Implementadas
Consistencia Transaccional (ACID): El pedido y el evento outbox se guardan juntos.
CDC en Tiempo Real: Latencia de < 100ms usando el WAL de PostgreSQL (sin consultas SQL constantes).
Consumidor Idempotente: Garantiza que un evento duplicado de Kafka no se procese dos veces.
Dead Letter Queue (DLQ): Los mensajes que fallan al procesarse se mueven a un tópico de cuarentena para evitar bloquear el flujo principal.
Limpieza Automática: Un proceso programado borra los eventos antiguos de la tabla outbox_events para mantener la base de datos optimizada.
Gestión de Tópicos como Código: Los tópicos de Kafka se crean automáticamente al iniciar la aplicación.
Tecnologías Utilizadas
Backend: Java 17+ / Spring Boot 3.2.x
Base de Datos: PostgreSQL 15 (Configurado con wal_level=logical)
CDC: Debezium 2.5 (Corriendo sobre Kafka Connect)
Message Broker: Apache Kafka 3.7 (Modo KRaft - sin ZooKeeper)
Build Tool: Gradle
Infraestructura: Docker & Docker Compose
Monitoreo: Kafka UI
🚀 Cómo Ejecutar
Prerrequisitos
Docker y Docker Compose.
Java 17+ y Gradle.
Paso 1: Levantar la Infraestructura
docker-compose up -d
ervicios: PostgreSQL (5432), Kafka (29092), Kafka Connect (8083), Kafka UI (8080).
Paso 2: Registrar el Conector Debezium
Una vez que los contenedores estén listos, registra el conector para que Debezium empiece a leer el WAL:
curl -i -X POST -H "Accept:application/json" \
  -H "Content-Type:application/json" \
  http://localhost:8083/connectors \
  -d @debezium-connector.json
Paso 3: Ejecutar la Aplicación Spring Boot
./gradlew bootRun
La API REST estará disponible en http://localhost:8081.
🧪 Pruebas y Validación
1. Flujo Exitoso (Happy Path)
Crea un pedido y observa cómo Debezium lo envía a Kafka y el consumidor lo procesa en milisegundos:
curl -X POST http://localhost:8081/api/pedidos \
-H "Content-Type: application/json" \
-d '{"cliente": "Juan Pérez", "total": 150.00}'
Verifica en Kafka UI (http://localhost:8080) que el mensaje llega al tópico principal y que la tabla consumed_events registra la idempotencia.
2. Prueba de Dead Letter Queue (DLQ)
Para simular un error, descomenta la línea if (Math.random() > 0.5) throw new RuntimeException(...) en InventoryConsumer.java. Al enviar pedidos, los que fallen se moverán automáticamente al tópico outbox-events-topic.DLQ.
📦 Estructura del Proyecto
src/main/java/com/ejemplo/outbox/
├── config/
│   └── KafkaTopicConfig.java          # Crea tópicos automáticamente
├── consumer/
│   └── InventoryConsumer.java         # Lógica de consumo, idempotencia y DLQ
├── controller/
│   └── PedidoController.java          # REST API
├── entity/
│   ├── Pedido.java                    # Entidad de negocio
│   ├── OutboxEvent.java               # Entidad del buzón
│   └── ConsumedEvent.java             # Entidad para idempotencia
── publisher/
│   └── OutboxPublisher.java           # (Opcional) Polling legacy
├── repository/
│   ├── PedidoRepository.java
│   ├── OutboxEventRepository.java     # Incluye query de limpieza
│   └── ConsumedEventRepository.java
── service/
    ├── PedidoService.java             # Transacción atómica
    └── OutboxCleaner.java             # Job programado de limpieza
📚 Recursos Adicionales
Debezium Documentation
Transactional Outbox Pattern
Kafka Connect
Proyecto desarrollado con fines educativos y de portafolio, demostrando patrones de arquitectura resiliente en microservicios.
