package com.riskflow.scoring.consumer;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import com.riskflow.scoring.dto.EnrichmentResult;
import com.riskflow.scoring.dto.TransactionEvent;
import com.riskflow.scoring.model.DecisionType;
import com.riskflow.scoring.model.RiskDecision;
import com.riskflow.scoring.repository.RiskDecisionRepository;
import com.riskflow.scoring.service.BehavioralAnalyzer;
import com.riskflow.scoring.service.ExternalEnrichmentService;
import com.riskflow.scoring.service.RuleEngine;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;

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

    private static final String DLQ_TOPIC = "transaction.scoring.failed";

    // The Tracer is our factory for creating Spans.
    // "riskflow-scoring" is the instrumentation scope name — it appears
    // in Jaeger to identify which component created each span.
    // The agent populates GlobalOpenTelemetry at JVM startup, so this
    // field is safe to initialize here even though it looks static-ish.
    private final Tracer tracer = GlobalOpenTelemetry
        .getTracer("riskflow-scoring", "1.0.0");

    private final RiskDecisionRepository decisionRepository;
    private final StringRedisTemplate redis;
    private final BehavioralAnalyzer behavioralAnalyzer;
    private final RuleEngine ruleEngine;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ExternalEnrichmentService enrichmentService;

    public TransactionEventConsumer(RiskDecisionRepository decisionRepository,
                                    StringRedisTemplate redis,
                                    BehavioralAnalyzer behavioralAnalyzer,
                                    RuleEngine ruleEngine,
                                    KafkaTemplate<String, String> kafkaTemplate,
                                    ExternalEnrichmentService enrichmentService) {
        this.decisionRepository = decisionRepository;
        this.redis = redis;
        this.behavioralAnalyzer = behavioralAnalyzer;
        this.ruleEngine = ruleEngine;
        this.kafkaTemplate = kafkaTemplate;
        this.enrichmentService = enrichmentService;
    }

    @KafkaListener(topics = "transaction.received")
    public void consume(
            @Payload String raw,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.debug("Received message from partition={} offset={}", partition, offset);

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

        // -----------------------------------------------------------------------
        // ROOT SPAN: score-transaction
        //
        // This is the parent span that wraps the entire scoring pipeline.
        // All four stage spans below are children of this span.
        //
        // How the span hierarchy works:
        //   1. We create the root span here.
        //   2. We open a Scope — this makes the root span "current" on this thread.
        //   3. Any span created inside the try block that calls
        //      tracer.spanBuilder(...).startSpan() automatically becomes a child
        //      because the builder reads the current span from thread context.
        //   4. We close the Scope in the finally block.
        //   5. We end the root span in the finally block.
        //
        // Why separate span.end() from scope.close()?
        // Scope is about thread context — it controls what is "current".
        // span.end() records the timing and sends the span to Jaeger.
        // They are different operations and must both happen.
        // -----------------------------------------------------------------------
        Span rootSpan = tracer.spanBuilder("score-transaction")
            .setSpanKind(SpanKind.CONSUMER)
            // Attributes appear as searchable key-value pairs in Jaeger.
            // Adding txnId here means you can search "transaction.id = txn_abc123"
            // in the Jaeger UI and find this exact trace immediately.
            .setAttribute("transaction.id", txnId)
            .setAttribute("transaction.amount", event.getAmount())
            .setAttribute("transaction.ip_country", event.getIpCountry())
            .startSpan();

        // makeCurrent() puts this span on the thread context.
        // The returned Scope must be closed — try-with-resources handles that.
        try (Scope rootScope = rootSpan.makeCurrent()) {

            // Step 2: Idempotency check
            String idempotencyKey = IDEMPOTENCY_KEY_PREFIX + txnId;
            Boolean claimed = redis.opsForValue()
                .setIfAbsent(idempotencyKey, "1", IDEMPOTENCY_TTL);

            if (!Boolean.TRUE.equals(claimed)) {
                log.info("Duplicate message detected. Already processed txnId={}. Skipping.", txnId);
                // Mark the root span as OK — duplicate detection is expected behavior
                rootSpan.setAttribute("skipped.reason", "duplicate");
                rootSpan.setStatus(StatusCode.OK);
                return;
            }

            // -------------------------------------------------------------------
            // STAGE 1 SPAN: hard-rules
            //
            // Wraps the three hard rule checks (blocklist, amount cap, countries).
            // If any hard rule fires, we mark this span with which rule triggered
            // so it's immediately visible in Jaeger without reading logs.
            // -------------------------------------------------------------------
            Span hardRulesSpan = tracer.spanBuilder("stage1-hard-rules")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("transaction.id", txnId)
                .startSpan();

            try (Scope hardRulesScope = hardRulesSpan.makeCurrent()) {

                if (BLOCKLISTED_CARDS.contains(event.getCardFingerprint())) {
                    log.warn("HARD_RULE: Blocklisted card. txnId={} card={}",
                        txnId, event.getCardFingerprint());
                    // Record which rule fired as a span attribute
                    hardRulesSpan.setAttribute("hard_rule.triggered", "BLOCKLIST_HIT");
                    hardRulesSpan.setAttribute("hard_rule.card", event.getCardFingerprint());
                    hardRulesSpan.setStatus(StatusCode.OK);
                    hardRulesSpan.end();
                    saveDecision(txnId, DecisionType.AUTO_REJECTED, 100,
                        "BLOCKLIST_HIT", "HARD_RULES");
                    rootSpan.setAttribute("decision", "AUTO_REJECTED");
                    rootSpan.setAttribute("decision.reason", "BLOCKLIST_HIT");
                    rootSpan.setStatus(StatusCode.OK);
                    return;
                }

                if (event.getAmount() > HARD_CAP_AMOUNT) {
                    log.warn("HARD_RULE: Amount exceeds cap. txnId={} amount={} cap={}",
                        txnId, event.getAmount(), HARD_CAP_AMOUNT);
                    hardRulesSpan.setAttribute("hard_rule.triggered", "AMOUNT_EXCEEDS_CAP");
                    hardRulesSpan.setAttribute("hard_rule.amount", event.getAmount());
                    hardRulesSpan.setStatus(StatusCode.OK);
                    hardRulesSpan.end();
                    saveDecision(txnId, DecisionType.AUTO_REJECTED, 100,
                        "AMOUNT_EXCEEDS_CAP", "HARD_RULES");
                    rootSpan.setAttribute("decision", "AUTO_REJECTED");
                    rootSpan.setAttribute("decision.reason", "AMOUNT_EXCEEDS_CAP");
                    rootSpan.setStatus(StatusCode.OK);
                    return;
                }

                if (SANCTIONED_COUNTRIES.contains(event.getIpCountry())) {
                    log.warn("HARD_RULE: Sanctioned country. txnId={} ipCountry={}",
                        txnId, event.getIpCountry());
                    hardRulesSpan.setAttribute("hard_rule.triggered", "SANCTIONED_COUNTRY");
                    hardRulesSpan.setAttribute("hard_rule.ip_country", event.getIpCountry());
                    hardRulesSpan.setStatus(StatusCode.OK);
                    hardRulesSpan.end();
                    saveDecision(txnId, DecisionType.AUTO_REJECTED, 100,
                        "SANCTIONED_COUNTRY", "HARD_RULES");
                    rootSpan.setAttribute("decision", "AUTO_REJECTED");
                    rootSpan.setAttribute("decision.reason", "SANCTIONED_COUNTRY");
                    rootSpan.setStatus(StatusCode.OK);
                    return;
                }

                // No hard rule triggered — mark span OK and record that
                hardRulesSpan.setAttribute("hard_rule.triggered", "NONE");
                hardRulesSpan.setStatus(StatusCode.OK);

            } finally {
                // Always end the span, even if an exception occurred inside.
                // A span that is never ended leaks memory and never appears in Jaeger.
                hardRulesSpan.end();
            }

            // -------------------------------------------------------------------
            // STAGE 2 SPAN: behavioral-analyzer
            //
            // Wraps the Redis sliding-window velocity checks.
            // We record the score contribution so you can see in Jaeger
            // exactly how much behavioral signals added to the total.
            // -------------------------------------------------------------------
            int behavioralScore;
            Span behavioralSpan = tracer.spanBuilder("stage2-behavioral-analyzer")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("transaction.id", txnId)
                .startSpan();

            try (Scope behavioralScope = behavioralSpan.makeCurrent()) {
                behavioralScore = behavioralAnalyzer.analyze(event);
                log.info("Behavioral score. txnId={} score={}", txnId, behavioralScore);
                // Record the score contribution as a span attribute
                behavioralSpan.setAttribute("behavioral.score", behavioralScore);
                behavioralSpan.setAttribute("behavioral.card_fingerprint", event.getCardFingerprint());
                behavioralSpan.setAttribute("behavioral.device_fingerprint", event.getDeviceFingerprint());
                behavioralSpan.setAttribute("behavioral.ip_address", event.getIpAddress());
                behavioralSpan.setStatus(StatusCode.OK);
            } finally {
                behavioralSpan.end();
            }

            // -------------------------------------------------------------------
            // STAGE 3 SPAN: rule-engine
            //
            // Wraps SpEL expression evaluation against all loaded rules.
            // We record how many rules were evaluated and the score contributed.
            // -------------------------------------------------------------------
            int ruleScore;
            Span ruleEngineSpan = tracer.spanBuilder("stage3-rule-engine")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("transaction.id", txnId)
                .startSpan();

            try (Scope ruleEngineScope = ruleEngineSpan.makeCurrent()) {
                ruleScore = ruleEngine.evaluate(event);
                log.info("Rule engine score. txnId={} score={}", txnId, ruleScore);
                ruleEngineSpan.setAttribute("rule_engine.score", ruleScore);
                ruleEngineSpan.setAttribute("rule_engine.mcc", event.getMerchantCategoryCode());
                ruleEngineSpan.setAttribute("rule_engine.merchant_risk_tier", event.getMerchantRiskTier());
                ruleEngineSpan.setStatus(StatusCode.OK);
            } finally {
                ruleEngineSpan.end();
            }

            // -------------------------------------------------------------------
            // STAGE 4 SPAN: external-enrichment
            //
            // Wraps the IP reputation call wrapped in Resilience4j.
            // This is the most valuable span to watch — if enrichment is slow,
            // its bar in Jaeger will be visibly wider than everything else.
            // We record the IP reputation result and score contribution.
            // -------------------------------------------------------------------
            int enrichScore = 0;
            Span enrichSpan = tracer.spanBuilder("stage4-external-enrichment")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("transaction.id", txnId)
                .setAttribute("enrichment.ip_address", event.getIpAddress())
                .startSpan();

            try (Scope enrichScope = enrichSpan.makeCurrent()) {
                try {
                    CompletableFuture<EnrichmentResult> enrichFuture =
                        enrichmentService.enrich(event.getIpAddress(), txnId);

                    EnrichmentResult enrichResult = enrichFuture.get();
                    enrichScore = enrichResult.toScore();

                    log.info("[ENRICHMENT] Result. txnId={} reputation={} score={}",
                        txnId, enrichResult.getReputation(), enrichScore);

                    // Record the reputation result as a searchable attribute
                    enrichSpan.setAttribute("enrichment.reputation",
                        enrichResult.getReputation().toString());
                    enrichSpan.setAttribute("enrichment.score", enrichScore);
                    enrichSpan.setStatus(StatusCode.OK);

                } catch (Exception e) {
                    log.warn("[ENRICHMENT] Unexpected error. txnId={} error={}. " +
                        "Proceeding with enrichScore=0.", txnId, e.getMessage());
                    // Mark this span as an error so it shows red in Jaeger,
                    // but the pipeline continues — enrichment failure is non-fatal.
                    enrichSpan.setStatus(StatusCode.ERROR, e.getMessage());
                    enrichSpan.recordException(e);
                }
            } finally {
                enrichSpan.end();
            }

            // -------------------------------------------------------------------
            // DECISION: combine all four stage scores and decide
            // -------------------------------------------------------------------
            int totalScore = behavioralScore + ruleScore + enrichScore;
            log.info("Total score. txnId={} behavioral={} rules={} enrichment={} total={}",
                txnId, behavioralScore, ruleScore, enrichScore, totalScore);

            DecisionType finalDecision;
            if (totalScore >= 60) {
                finalDecision = DecisionType.AUTO_REJECTED;
            } else if (totalScore >= 20) {
                finalDecision = DecisionType.NEEDS_REVIEW;
            } else {
                finalDecision = DecisionType.APPROVED;
            }

            // Record the final decision and all stage scores on the root span.
            // This means when you open any trace in Jaeger, the top-level span
            // immediately shows you the outcome without clicking into children.
            rootSpan.setAttribute("decision", finalDecision.toString());
            rootSpan.setAttribute("score.behavioral", behavioralScore);
            rootSpan.setAttribute("score.rules", ruleScore);
            rootSpan.setAttribute("score.enrichment", enrichScore);
            rootSpan.setAttribute("score.total", totalScore);
            rootSpan.setStatus(StatusCode.OK);

            saveDecision(txnId, finalDecision, totalScore, "RULE_ENGINE", "STAGE_4");

        } catch (Exception e) {
            log.error("Unexpected error scoring txnId={}. Publishing to DLQ. error={}",
                txnId, e.getMessage());
            // Mark the root span as ERROR — this transaction will appear red in Jaeger
            rootSpan.setStatus(StatusCode.ERROR, e.getMessage());
            rootSpan.recordException(e);
            publishToDlq(txnId, raw, e.getMessage());
        } finally {
            // Always end the root span. If we return early (hard rules, duplicate),
            // the finally block still runs and the span is properly closed.
            rootSpan.end();
        }
    }

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