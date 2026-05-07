package com.riskflow.scoring.service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.riskflow.scoring.dto.EnrichmentResult;
import com.riskflow.scoring.dto.EnrichmentResult.Reputation;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import io.github.resilience4j.timelimiter.annotation.TimeLimiter;

/**
 * Stage 4 of the scoring pipeline — external IP reputation enrichment.
 *
 * This service wraps a simulated external IP reputation lookup with three
 * layers of Resilience4j fault tolerance:
 *
 * Layer 1 — @Retry("ip-reputation")
 *   Retries the call up to maxAttempts times before giving up.
 *   Configured in application.properties:
 *     maxAttempts=2, waitDuration=200ms
 *
 * Layer 2 — @TimeLimiter("ip-reputation")
 *   Cancels the call if it takes longer than timeoutDuration.
 *   Configured: timeoutDuration=2s
 *   IMPORTANT: TimeLimiter requires the method to return CompletableFuture.
 *   This is the only Resilience4j decorator with this requirement.
 *
 * Layer 3 — @CircuitBreaker("ip-reputation", fallbackMethod="enrichmentFallback")
 *   Tracks failures. If failure rate exceeds threshold, opens the circuit
 *   and routes all calls directly to the fallback without attempting the call.
 *   Configured: slidingWindowSize=10, failureRateThreshold=50, waitDuration=10s
 *
 * Execution order when all three are stacked:
 *   CircuitBreaker checks state first.
 *   If OPEN → fallback immediately, no call attempted.
 *   If CLOSED/HALF_OPEN → Retry wraps the attempt.
 *     Each attempt is wrapped by TimeLimiter.
 *     If attempt times out → TimeLimiter throws TimeoutException.
 *     Retry catches it and tries again (up to maxAttempts).
 *     If all retries exhausted → CircuitBreaker records the failure.
 *     If failure rate crosses threshold → circuit opens.
 *
 * The fallback always returns EnrichmentResult.fallback() (reputation=CLEAN, score=0).
 * This means enrichment failure is non-fatal — the pipeline continues with
 * the signals it already has from Stages 1-3.
 *
 * --- SIMULATION BEHAVIOUR ---
 * The simulated client has three modes controlled by the IP address prefix:
 *
 *   IP starts with "10."      → always returns MALICIOUS (score +40)
 *   IP starts with "172."     → always returns SUSPICIOUS (score +20)
 *   IP starts with "fail."    → throws RuntimeException (triggers retry + CB)
 *   IP starts with "slow."    → sleeps 3 seconds (triggers time limiter)
 *   Everything else           → returns CLEAN (score +0)
 *
 * To demo the circuit breaker opening:
 *   Send 5+ transactions with ipAddress starting with "fail." or "slow."
 *   Watch the logs for: CircuitBreaker 'ip-reputation' changed state to OPEN
 *   Then send a normal transaction — it goes straight to fallback with no delay.
 */
@Service
public class ExternalEnrichmentService {

    private static final Logger log = LoggerFactory.getLogger(ExternalEnrichmentService.class);

    // The Resilience4j instance name — must match application.properties keys:
    //   resilience4j.circuitbreaker.instances.ip-reputation.*
    //   resilience4j.timelimiter.instances.ip-reputation.*
    //   resilience4j.retry.instances.ip-reputation.*
    private static final String RESILIENCE4J_INSTANCE = "ip-reputation";

    /**
     * Enrich a transaction with IP reputation data.
     *
     * Returns CompletableFuture because @TimeLimiter requires async execution.
     * CompletableFuture.supplyAsync() runs the actual work on a separate thread
     * from the ForkJoinPool. The TimeLimiter cancels that future if it does not
     * complete within the configured timeout.
     *
     * The three annotations stack in this order (outermost to innermost):
     *   CircuitBreaker → Retry → TimeLimiter → actual method body
     *
     * @param ipAddress  the IP address from the transaction event
     * @param txnId      used only for log messages
     * @return           CompletableFuture that resolves to an EnrichmentResult
     */
    @CircuitBreaker(name = RESILIENCE4J_INSTANCE, fallbackMethod = "enrichmentFallback")
    @Retry(name = RESILIENCE4J_INSTANCE)
    @TimeLimiter(name = RESILIENCE4J_INSTANCE)
    public CompletableFuture<EnrichmentResult> enrich(String ipAddress, String txnId) {

        // CompletableFuture.supplyAsync() submits the work to a thread pool
        // and returns immediately with a Future that will hold the result.
        // The TimeLimiter monitors this future and cancels it on timeout.
        return CompletableFuture.supplyAsync(() -> {
            log.info("[ENRICHMENT] Calling IP reputation service. txnId={} ip={}", txnId, ipAddress);

            // --- Simulated external call behaviour ---

            // Simulate a slow response — triggers the TimeLimiter
            if (ipAddress != null && ipAddress.startsWith("slow.")) {
                try {
                    log.warn("[ENRICHMENT] Simulating slow response (3s). txnId={}", txnId);
                    TimeUnit.SECONDS.sleep(3);  // TimeLimiter timeout is 2s — this will be cancelled
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Enrichment interrupted", e);
                }
            }

            // Simulate a failure — triggers Retry then Circuit Breaker
            if (ipAddress != null && ipAddress.startsWith("fail.")) {
                log.warn("[ENRICHMENT] Simulating service failure. txnId={}", txnId);
                throw new RuntimeException("IP reputation service unavailable (simulated)");
            }

            // Simulate a MALICIOUS result
            if (ipAddress != null && ipAddress.startsWith("10.")) {
                log.info("[ENRICHMENT] IP flagged as MALICIOUS. txnId={} ip={}", txnId, ipAddress);
                return EnrichmentResult.of(Reputation.MALICIOUS, "simulated");
            }

            // Simulate a SUSPICIOUS result
            if (ipAddress != null && ipAddress.startsWith("172.")) {
                log.info("[ENRICHMENT] IP flagged as SUSPICIOUS. txnId={} ip={}", txnId, ipAddress);
                return EnrichmentResult.of(Reputation.SUSPICIOUS, "simulated");
            }

            // Default: CLEAN
            log.info("[ENRICHMENT] IP is CLEAN. txnId={} ip={}", txnId, ipAddress);
            return EnrichmentResult.of(Reputation.CLEAN, "simulated");
        });
    }

    /**
     * Fallback method — called by the CircuitBreaker when:
     *   1. The circuit is OPEN (failure rate exceeded threshold), OR
     *   2. All retry attempts are exhausted, OR
     *   3. The TimeLimiter cancels the call due to timeout
     *
     * IMPORTANT: The fallback method signature must match the primary method
     * exactly, plus an additional Throwable parameter at the end.
     * Resilience4j uses reflection to find the fallback — wrong signature
     * means it silently fails to wire and you get an uncaught exception instead.
     *
     * We return EnrichmentResult.fallback() which has reputation=CLEAN and score=0.
     * This means enrichment failures add nothing to the score — the pipeline
     * continues with Stage 1-3 signals only. No penalty for external failure.
     */
    public CompletableFuture<EnrichmentResult> enrichmentFallback(String ipAddress,
                                                                    String txnId,
                                                                    Throwable t) {
        log.warn("[ENRICHMENT FALLBACK] Enrichment unavailable. txnId={} ip={} reason={}",
            txnId, ipAddress, t.getMessage());

        // CompletableFuture.completedFuture() wraps a value in an already-completed
        // future — no async execution, returns immediately.
        return CompletableFuture.completedFuture(EnrichmentResult.fallback());
    }
}