package com.riskflow.ingestion.service;

import java.time.Instant;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.riskflow.ingestion.model.OutboxEvent;
import com.riskflow.ingestion.model.OutboxStatus;
import com.riskflow.ingestion.repository.OutboxEventRepository;

// The OutboxPoller is the relay between PostgreSQL and Kafka.
// It runs on a fixed schedule, finds all PENDING outbox records,
// publishes each one to Kafka, then marks it as PUBLISHED.
//
// This is the only class in the ingestion service that knows Kafka exists.
// TransactionService writes to the outbox. OutboxPoller reads from it.
// Clean separation of concerns.
@Component
public class OutboxPoller {

    private static final Logger log = LoggerFactory.getLogger(OutboxPoller.class);

    // The Kafka topic the Risk Scoring Service listens on
    private static final String TOPIC = "transaction.received";

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OutboxPoller(OutboxEventRepository outboxEventRepository,
                        KafkaTemplate<String, String> kafkaTemplate) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    // @Scheduled tells Spring to call this method automatically on a fixed interval.
    // fixedDelay = 1000 means: wait 1000ms after the previous run finishes,
    // then run again. This prevents overlapping runs if processing takes longer
    // than the interval.
    //
    // For this to work, @EnableScheduling must be on the application class.
    // We will add that next.
    @Scheduled(fixedDelay = 1000)
    // @Transactional here ensures that if anything fails mid-loop,
    // the status updates are rolled back. The records stay PENDING
    // and the poller will retry them on the next run.
    @Transactional
    public void pollAndPublish() {

        // Fetch all outbox records that have not been published yet
        List<OutboxEvent> pendingEvents =
            outboxEventRepository.findByStatus(OutboxStatus.PENDING);

        if (pendingEvents.isEmpty()) {
            // Nothing to do — this will happen most of the time
            return;
        }

        log.info("OutboxPoller: found {} pending event(s) to publish", pendingEvents.size());

        for (OutboxEvent event : pendingEvents) {

            // Publish to Kafka using the pre-built payload stored in the outbox.
            // The transactionId is the Kafka message key — Kafka uses this to
            // route messages to partitions, ensuring ordering per transaction.
            kafkaTemplate.send(TOPIC, event.getTransactionId(), event.getPayload());

            // Mark the record as published so the poller skips it next time
            event.setStatus(OutboxStatus.PUBLISHED);
            event.setPublishedAt(Instant.now());
            outboxEventRepository.save(event);

            log.info("OutboxPoller: published transactionId={}", event.getTransactionId());
        }
    }
}