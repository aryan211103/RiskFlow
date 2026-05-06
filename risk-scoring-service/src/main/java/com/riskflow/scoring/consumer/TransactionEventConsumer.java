package com.riskflow.scoring.consumer;

import java.time.Duration;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import com.riskflow.scoring.dto.TransactionEvent;
import com.riskflow.scoring.model.DecisionType;
import com.riskflow.scoring.model.RiskDecision;
import com.riskflow.scoring.repository.RiskDecisionRepository;
import com.riskflow.scoring.service.BehavioralAnalyzer;
import com.riskflow.scoring.service.RuleEngine;

@Component
public class TransactionEventConsumer {

    private static final Logger log =
        LoggerFactory.getLogger(TransactionEventConsumer.class);

    private static final double HARD_CAP_AMOUNT = 10_000.0;

    private static final Set<String> SANCTIONED_COUNTRIES =
        Set.of("RU", "KP", "IR", "BY", "SY");

    private static final Set<String> BLOCKLISTED_CARDS =
        Set.of("fpr_BLOCKED_001", "fpr_BLOCKED_002", "fpr_BLOCKED_003");

    private static final String IDEMPOTENCY_KEY_PREFIX = "risk:processed:";
    private static final Duration IDEMPOTENCY_TTL = Duration.ofHours(24);

    // The DLQ topic — any unrecoverable failure gets published here.
    // The DLQ Processor Service listens on this topic.
    private static final String DLQ_TOPIC = "transaction.scoring.failed";

    private final RiskDecisionRepository decisionRepository;
    private final StringRedisTemplate redis;
    private final BehavioralAnalyzer behavioralAnalyzer;
    private final RuleEngine ruleEngine;

    // KafkaTemplate is now injected so we can publish to the DLQ topic.
    // Spring auto-configures this bean using the producer properties
    // already in application.properties — no extra config needed.
    private final KafkaTemplate<String, String> kafkaTemplate;

    public TransactionEventConsumer(RiskDecisionRepository decisionRepository,
                                    StringRedisTemplate redis,
                                    BehavioralAnalyzer behavioralAnalyzer,
                                    RuleEngine ruleEngine,
                                    KafkaTemplate<String, String> kafkaTemplate) {
        this.decisionRepository = decisionRepository;
        this.redis = redis;
        this.behavioralAnalyzer = behavioralAnalyzer;
        this.ruleEngine = ruleEngine;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "transaction.received")
    public void consume(
            @Payload String raw,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.debug("Received message from partition={} offset={}", partition, offset);

        // Step 1: Parse the raw payload.
        // A parse failure is a poison pill — the payload is malformed and
        // will never succeed. Publish to DLQ immediately and stop.
        TransactionEvent event;
        try {
            event = TransactionEvent.from(raw);
        } catch (IllegalArgumentException e) {
            log.error("Failed to parse payload. Publishing to DLQ. raw={} error={}",
                raw, e.getMessage());
            publishToDlq("UNKNOWN", raw, e.getMessage());
            return;
        }

        String txnId = event.getTransactionId();
        log.info("Processing transaction txnId={} amount={} ipCountry={} card={}",
            txnId, event.getAmount(), event.getIpCountry(), event.getCardFingerprint());

        // Wrap the entire scoring pipeline in a try-catch.
        // Any unexpected exception — Redis down, database blip, null pointer —
        // gets caught here and routed to the DLQ instead of being silently dropped
        // or blocking the consumer.
        try {

            // Step 2: Idempotency check
            String idempotencyKey = IDEMPOTENCY_KEY_PREFIX + txnId;
            Boolean claimed = redis.opsForValue()
                .setIfAbsent(idempotencyKey, "1", IDEMPOTENCY_TTL);

            if (!Boolean.TRUE.equals(claimed)) {
                log.info("Duplicate message detected. Already processed txnId={}. Skipping.", txnId);
                return;
            }

            log.debug("Idempotency claim successful for txnId={}", txnId);

            // Step 3: Stage 1 — Hard Rules
            if (BLOCKLISTED_CARDS.contains(event.getCardFingerprint())) {
                log.warn("HARD_RULE: Blocklisted card. txnId={} card={}",
                    txnId, event.getCardFingerprint());
                saveDecision(txnId, DecisionType.AUTO_REJECTED, 100,
                    "BLOCKLIST_HIT", "HARD_RULES");
                return;
            }

            if (event.getAmount() > HARD_CAP_AMOUNT) {
                log.warn("HARD_RULE: Amount exceeds cap. txnId={} amount={} cap={}",
                    txnId, event.getAmount(), HARD_CAP_AMOUNT);
                saveDecision(txnId, DecisionType.AUTO_REJECTED, 100,
                    "AMOUNT_EXCEEDS_CAP", "HARD_RULES");
                return;
            }

            if (SANCTIONED_COUNTRIES.contains(event.getIpCountry())) {
                log.warn("HARD_RULE: Sanctioned country. txnId={} ipCountry={}",
                    txnId, event.getIpCountry());
                saveDecision(txnId, DecisionType.AUTO_REJECTED, 100,
                    "SANCTIONED_COUNTRY", "HARD_RULES");
                return;
            }

            // Step 4: Stage 2 — Behavioral Analysis
            int behavioralScore = behavioralAnalyzer.analyze(event);
            log.info("Behavioral score. txnId={} score={}", txnId, behavioralScore);

            // Step 5: Stage 3 — Rule Engine
            int ruleScore = ruleEngine.evaluate(event);
            int totalScore = behavioralScore + ruleScore;
            log.info("Total score. txnId={} behavioral={} rules={} total={}",
                txnId, behavioralScore, ruleScore, totalScore);

            // Final decision
            DecisionType finalDecision;
            if (totalScore >= 60) {
                finalDecision = DecisionType.AUTO_REJECTED;
            } else if (totalScore >= 20) {
                finalDecision = DecisionType.NEEDS_REVIEW;
            } else {
                finalDecision = DecisionType.APPROVED;
            }

            saveDecision(txnId, finalDecision, totalScore, "RULE_ENGINE", "STAGE_3");

        } catch (Exception e) {
            // Something unexpected failed inside the scoring pipeline.
            // Publish to the DLQ so the DLQ Processor can classify and retry.
            log.error("Unexpected error scoring txnId={}. Publishing to DLQ. error={}",
                txnId, e.getMessage());
            publishToDlq(txnId, raw, e.getMessage());
        }
    }

    // Publishes a failed message to the DLQ topic.
    // The error message is sent as a Kafka header so the DLQ Processor
    // can read it without needing to parse the payload.
    private void publishToDlq(String transactionId, String payload, String errorMessage) {
        org.apache.kafka.clients.producer.ProducerRecord<String, String> record =
            new org.apache.kafka.clients.producer.ProducerRecord<>(
                DLQ_TOPIC, null, transactionId, payload
            );
        record.headers().add("error-message",
            (errorMessage != null ? errorMessage : "Unknown error").getBytes());
    
        kafkaTemplate.send(record);
        log.info("Published to DLQ. txnId={}", transactionId);
    }

    private void saveDecision(String transactionId,
                               DecisionType decision,
                               int score,
                               String reason,
                               String stage) {
        RiskDecision rd = new RiskDecision(transactionId, decision, score, reason, stage);
        decisionRepository.save(rd);
        log.info("Decision saved. txnId={} decision={} score={} reason={}",
            transactionId, decision, score, reason);
    }
}