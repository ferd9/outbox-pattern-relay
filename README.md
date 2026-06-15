# Outbox Pattern - Implementación con Spring Boot y Kafka

[![Java](https://img.shields.io/badge/Java-17+-blue.svg)](https://openjdk.java.net/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.2.x-brightgreen.svg)](https://spring.io/projects/spring-boot)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.7-red.svg)](https://kafka.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue.svg)](https://www.postgresql.org/)

## 📋 Propósito

Este proyecto implementa el **Outbox Pattern**, un patrón de diseño esencial en arquitecturas de microservicios que garantiza la **entrega confiable de eventos** en sistemas distribuidos. Su objetivo principal es resolver el problema de la **doble escritura** (Dual Write Problem) y asegurar la consistencia eventual entre la base de datos transaccional y el broker de mensajes.

## 🎯 Problema que Resuelve

En arquitecturas basadas en eventos, cuando un servicio necesita:
1. Guardar el estado de negocio en la base de datos.
2. Publicar un evento en un message broker (Kafka, RabbitMQ, etc.) para notificar a otros servicios.

El enfoque ingenuo (hacer ambas operaciones por separado) puede causar:
- ❌ **Pérdida de mensajes**: Si la base de datos guarda el registro pero el broker de mensajes falla o está caído.
- ❌ **Mensajes fantasma**: Si el broker publica el evento pero la base de datos falla al guardar el registro.
- ❌ **Inconsistencia de datos**: Estados divergentes entre los distintos microservicios.

**Solución**: El Outbox Pattern garantiza que ambas operaciones (guardar el dato de negocio y el evento) sean atómicas utilizando una única transacción local de base de datos.

## 🏗️ Arquitectura

```text
┌─────────────┐     ┌──────────────────────┐     ┌─────────────────┐
│   Cliente   │────▶│   Spring Boot App    │────▶│   PostgreSQL    │
│  (Frontend) │     │                      │     │                 │
└─────────────┘     │  - PedidoService     │     │  - Tabla pedidos│
                    │  - OutboxPublisher   │     │  - Tabla outbox │
                    └──────────────────────┘     └─────────────────┘
                                  │
                                  │ Polling (cada 5s)
                                  ▼
                           ┌──────────────┐
                           │    Kafka     │
                           │ outbox-events│
                           │    -topic    │
                           ──────────────┘

🚀 Cómo Ejecutar
Prerrequisitos
Docker y Docker Compose instalados.
Java 17 o superior instalado.
Gradle (o usar el wrapper gradlew incluido en el proyecto).
Paso 1: Levantar la Infraestructura
Abre una terminal en la raíz del proyecto y ejecuta:

# Iniciar PostgreSQL, Kafka y Kafka UI
docker-compose up -d

# Verificar que los contenedores estén corriendo correctamente
docker-compose ps

Servicios disponibles:
PostgreSQL: localhost:5432 (Usuario: admin, Contraseña: admin123, DB: outbox_db)
Kafka Broker: localhost:29092
Kafka UI: http://localhost:8080
Paso 2: Ejecutar la Aplicación Spring Boot
En otra terminal, ejecuta la aplicación:

# Usando Gradle wrapper (Linux/Mac)
./gradlew bootRun

# Usando Gradle wrapper (Windows)
gradlew.bat bootRun

La API REST estará disponible en: http://localhost:8081
Paso 3: Probar el Endpoint
Envía una petición POST para crear un pedido:

curl -X POST http://localhost:8081/api/pedidos \
-H "Content-Type: application/json" \
-d '{
  "cliente": "Juan Pérez",
  "total": 350.50
}'

Paso 4: Verificar el Resultado
Logs de Spring Boot: Espera unos 5 segundos y observa en la consola:
Polling Publisher encontró 1 eventos pendientes de publicar.
✅ Evento publicado en Kafka: <uuid-del-evento>
Kafka UI: Abre tu navegador en http://localhost:8080 → Ve a Topics → outbox-events-topic → Pestaña Messages. Verás el payload JSON.
Base de Datos: Verifica que el estado del evento cambió a PROCESSED:
sql

SELECT id, aggregate_id, event_type, estado FROM outbox_events ORDER BY created_at DESC LIMIT 1;

📖 Explicación del Patrón
Flujo de Trabajo Interno
Transacción Atómica (@Transactional):

// 1. Guardar el dato de negocio
pedidoRepository.save(pedido);

// 2. Guardar el evento en la tabla outbox con estado PENDING
outboxEventRepository.save(evento);

// ¡Gracias a @Transactional, ambos se guardan o ninguno! (ACID)

Polling Publisher (@Scheduled)
// Cada 5 segundos (configurable):
// 1. Hacer SELECT buscando eventos con estado = PENDING
// 2. Publicar el payload en el tópico de Kafka
// 3. Si Kafka confirma la recepción, actualizar estado a PROCESSED

Ventajas Clave de esta Implementación
✅ Consistencia Garantizada: Las transacciones ACID eliminan el Dual Write Problem.
✅ Entrega Confiable (At-least-once): Si Kafka está caído, el Polling reintentará automáticamente cada X segundos hasta que esté disponible.
✅ Sin Dependencia Externa Compleja: No requiere Two-Phase Commit (2PC) ni transacciones distribuidas pesadas.
✅ Orden Preservado: Se utiliza el aggregate_id (ID del pedido) como Key de Kafka, garantizando el orden de los eventos por entidad.
✅ Idempotencia Preparada: El id único del evento permite a los consumidores deduplicar mensajes si se envían más de una vez.
Consideraciones para Entornos de Producción
Si llevas este código a producción, se recomienda implementar:
Idempotencia en el Consumidor: El servicio que lea de Kafka debe verificar si ya procesó ese id de evento antes de ejecutar la lógica de negocio.
Limpieza de la Tabla Outbox: Un job programado que borre o archive registros con estado PROCESSED antiguos para evitar que la tabla crezca infinitamente.
Manejo de Concurrencia: Si escalas horizontalmente (múltiples instancias de la app), usa SELECT ... FOR UPDATE SKIP LOCKED en PostgreSQL para evitar que dos instancias procesen el mismo evento simultáneamente.
Dead Letter Queue (DLQ): Mover a una cola de errores los mensajes que fallen después de un número máximo de reintentos.
🔍 Configuración Importante
application.properties

# Puerto de la aplicación (cambiado a 8081 para no chocar con Kafka UI)
server.port=8081

# Conexión a PostgreSQL
spring.datasource.url=jdbc:postgresql://localhost:5432/outbox_db
spring.datasource.username=admin
spring.datasource.password=admin123
spring.jpa.hibernate.ddl-auto=update

# Conexión a Kafka
spring.kafka.bootstrap-servers=localhost:29092
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer

# Tópico de Kafka
kafka.topic.outbox=outbox-events-topic

# Intervalo de polling en milisegundos (default: 5000ms)
outbox.polling.interval=5000