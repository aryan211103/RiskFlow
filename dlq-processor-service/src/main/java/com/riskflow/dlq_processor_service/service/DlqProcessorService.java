package com.riskflow.dlq_processor_service.service;

import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.riskflow.dlq_processor_service.model.FailedEvent;
import com.riskflow.dlq_processor_service.model.FailedEventStatus;
import com.riskflow.dlq_processor_service.model.FailureType;
import com.riskflow.dlq_processor_service.repository.FailedEventRepository;

// Core logic of the DLQ Processor.
// Responsibilities:
//   1. Persist every incoming failed event to PostgreSQL
//   2. Classify it as TRANSIENT or POISON_PILL
//   3. For TRANSIENT: retry by replaying to the original topic
//   4. For POISON_PILL: quarantine immediately
//   5. Track retry counts and quarantine after max retries
@Service
public class DlqProcessorService {

    private static final Logger log = LoggerFactory.getLogger(DlqProcessorService.class);

    // The original topic the scoring service listens on.
    // Retrying means republishing the payload back here.
    private static final String SCORING_TOPIC = "transaction.received";

    // Maximum number of retry attempts before quarantining a transient failure.
    // After 3 attempts we give up — the infrastructure problem has lasted too long.
    private static final int MAX_RETRIES = 3;

    private final FailedEventRepository failedEventRepository;
    private final FailureClassifier failureClassifier;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public DlqProcessorService(FailedEventRepository failedEventRepository,
                                FailureClassifier failureClassifier,
                                KafkaTemplate<String, String> kafkaTemplate) {
        this.failedEventRepository = failedEventRepository;
        this.failureClassifier = failureClassifier;
        this.kafkaTemplate = kafkaTemplate;
    }

    // Called by the Kafka consumer every time a message arrives on the DLQ topic.
    // @Transactional ensures the database write and status update are atomic.
    @Transactional
    public void process(String transactionId, String payload, String errorMessage) {

        // Idempotency check — if we have already recorded this failure,
        // skip it. Kafka at-least-once delivery means we might see it twice.
        if (failedEventRepository.existsByTransactionId(transactionId)) {
            log.warn("DlqProcessor: duplicate DLQ message for txnId={}, skipping", transactionId);
            return;
        }

        // Classify the failure before persisting so we can set the right type
        FailureType failureType = failureClassifier.classify(errorMessage);

        // Persist the failed event — this is our audit record of the failure
        FailedEvent event = new FailedEvent();
        event.setTransactionId(transactionId);
        event.setOriginalPayload(payload);
        event.setErrorMessage(errorMessage);
        event.setFailureType(failureType);
        // status defaults to PENDING in the entity

        FailedEvent saved = failedEventRepository.save(event);

        log.info("DlqProcessor: recorded failure txnId={} type={}", transactionId, failureType);

        // Route based on classification
        if (failureType == FailureType.POISON_PILL) {
            quarantine(saved, "Classified as poison pill on first attempt");
        } else {
            retry(saved);
        }
    }

    // Attempts to replay the failed event by republishing to the scoring topic.
    // If max retries exceeded, quarantines instead.
    private void retry(FailedEvent event) {

        if (event.getRetryCount() >= MAX_RETRIES) {
            quarantine(event, "Max retries (" + MAX_RETRIES + ") exceeded");
            return;
        }

        log.info("DlqProcessor: retrying txnId={} attempt={}",
            event.getTransactionId(), event.getRetryCount() + 1);

        event.setStatus(FailedEventStatus.RETRYING);
        event.setRetryCount(event.getRetryCount() + 1);
        event.setLastRetriedAt(Instant.now());
        failedEventRepository.save(event);

        // Replay the original payload back to the scoring topic.
        // The scoring service's idempotency check will catch true duplicates.
        kafkaTemplate.send(SCORING_TOPIC, event.getTransactionId(), event.getOriginalPayload());

        log.info("DlqProcessor: replayed txnId={} to {}",
            event.getTransactionId(), SCORING_TOPIC);
    }

    // Marks the event as QUARANTINED — no more retries.
    // A human analyst needs to review this record in the database.
    private void quarantine(FailedEvent event, String reason) {

        log.warn("DlqProcessor: quarantining txnId={} reason={}",
            event.getTransactionId(), reason);

        event.setStatus(FailedEventStatus.QUARANTINED);
        event.setResolvedAt(Instant.now());
        failedEventRepository.save(event);
    }
}