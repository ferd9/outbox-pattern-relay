package org.elparendiz.core.outboxpatternrelay.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elparendiz.core.outboxpatternrelay.repository.OutboxEventRepository;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
@Slf4j
public class OutboxCleaner {

    private final OutboxEventRepository outboxEventRepository;

    /**
     * Se ejecuta todos los días a las 2:00 AM.
     * Puedes cambiar el cron según tus necesidades.
     * Ejemplos:
     * "0 0 2 * * *" -> Todos los días a las 2 AM
     * "0 0 *6 * * *" -> Cada 6 horas * "0 0 12 * * MON-FRI" -> Todos los días laborables a las 12 PM
     */
    //@Scheduled(cron = "0 0 2 * * *")
    @Scheduled(fixedDelay = 10000) // Cada 10 segundos
    @Transactional
    public void cleanupOldEvents() {
        log.info(" Iniciando limpieza automática de la tabla outbox_events...");

        // Definimos el umbral: borrar eventos de hace más de 7 días
        LocalDateTime threshold = LocalDateTime.now().minusDays(1);

        try {
            int deletedCount = outboxEventRepository.deleteOlderThan(threshold);
            log.info(" Limpieza completada. Se eliminaron {} eventos antiguos (anteriores a {}).",
                    deletedCount, threshold);
        } catch (Exception e) {
            log.error(" Error durante la limpieza de la tabla outbox_events: {}", e.getMessage(), e);
        }
    }
}