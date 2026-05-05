package com.riskflow.scoring.service;

import java.time.Instant;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import com.riskflow.scoring.dto.TransactionEvent;

@Service
public class BehavioralAnalyzer {

    // RedisTemplate is Spring's wrapper for all Redis operations.
    // The two type parameters mean: keys are Strings, values are Strings.
    // Spring auto-wires this bean because we have spring-boot-starter-data-redis
    // in our pom.xml and Redis connection properties in application.properties.
    private final StringRedisTemplate redisTemplate;

    public BehavioralAnalyzer(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    // -------------------------------------------------------------------------
    // PUBLIC ENTRY POINT
    // Called by TransactionEventConsumer after Stage 1 hard rules pass.
    // Returns a total integer score contribution from all behavioral signals.
    // -------------------------------------------------------------------------
    public int analyze(TransactionEvent event) {
        int totalScore = 0;

        // Stage 2a: how fast is this specific card being used?
        totalScore += scoreCardVelocity(event);

        // Stage 2b: how many distinct cards has this device touched today?
        totalScore += scoreDeviceCrossCard(event);

        // Stage 2c: how many distinct cards has this IP touched in the last hour?
        totalScore += scoreIpCrossCard(event);

        return totalScore;
    }

    // -------------------------------------------------------------------------
    // STAGE 2a — PER-CARD VELOCITY
    // Checks three sliding windows for the card fingerprint.
    // A real human uses a card a few times a day at a human pace.
    // A fraudster burns a stolen card as fast as possible.
    // -------------------------------------------------------------------------
    private int scoreCardVelocity(TransactionEvent event) {
        String cardFp = event.getCardFingerprint();
        String txnId  = event.getTransactionId();
        int score = 0;

        // Record this transaction in all three card velocity windows.
        // We store the transactionId as the member so each event is unique.
        // The Unix timestamp in milliseconds is the score — this is what
        // allows us to do time-range queries later.
        long nowMs = Instant.now().toEpochMilli();

        // --- 1-minute window ---
        // Key pattern: risk:velocity:card:{fingerprint}:1m
        String key1m = "risk:velocity:card:" + cardFp + ":1m";
        long window1m = 60_000L; // 60 seconds in milliseconds

        // Add this transaction to the sorted set with current timestamp as score
        redisTemplate.opsForZSet().add(key1m, txnId, nowMs);

        // Remove all entries older than 1 minute — keeps the set clean
        // ZREMRANGEBYSCORE key -inf (nowMs - 60000)
        redisTemplate.opsForZSet().removeRangeByScore(key1m, 0, nowMs - window1m);

        // Count how many transactions remain in the window
        Long count1m = redisTemplate.opsForZSet().zCard(key1m);
        long cardCount1m = (count1m != null) ? count1m : 0L;

        // Set TTL so Redis auto-cleans this key if the card goes quiet.
        // 2 minutes is enough — if no activity for 2m, the 1m window is empty anyway.
        redisTemplate.expire(key1m, 2, java.util.concurrent.TimeUnit.MINUTES);

        // Score contribution for 1-minute window:
        // 3+ transactions in 1 minute from the same card is highly suspicious
        if (cardCount1m >= 5) score += 30;
        else if (cardCount1m >= 3) score += 15;

        // --- 5-minute window ---
        String key5m = "risk:velocity:card:" + cardFp + ":5m";
        long window5m = 300_000L; // 5 minutes in milliseconds

        redisTemplate.opsForZSet().add(key5m, txnId, nowMs);
        redisTemplate.opsForZSet().removeRangeByScore(key5m, 0, nowMs - window5m);
        Long count5m = redisTemplate.opsForZSet().zCard(key5m);
        long cardCount5m = (count5m != null) ? count5m : 0L;
        redisTemplate.expire(key5m, 10, java.util.concurrent.TimeUnit.MINUTES);

        // 6+ transactions in 5 minutes is a strong signal
        if (cardCount5m >= 8) score += 25;
        else if (cardCount5m >= 6) score += 15;

        // --- 1-hour window ---
        String key1h = "risk:velocity:card:" + cardFp + ":1h";
        long window1h = 3_600_000L; // 1 hour in milliseconds

        redisTemplate.opsForZSet().add(key1h, txnId, nowMs);
        redisTemplate.opsForZSet().removeRangeByScore(key1h, 0, nowMs - window1h);
        Long count1h = redisTemplate.opsForZSet().zCard(key1h);
        long cardCount1h = (count1h != null) ? count1h : 0L;
        redisTemplate.expire(key1h, 2, java.util.concurrent.TimeUnit.HOURS);

        // 15+ transactions in 1 hour is abnormal for any legitimate card
        if (cardCount1h >= 20) score += 20;
        else if (cardCount1h >= 15) score += 10;

        return score;
    }

    // -------------------------------------------------------------------------
    // STAGE 2b — PER-DEVICE CROSS-CARD TRACKING
    // Counts how many DISTINCT card fingerprints this device has used in 24h.
    // A real user has 1-2 cards on a device.
    // A fraudster's laptop cycles through dozens of stolen cards.
    //
    // KEY DIFFERENCE from card velocity:
    // The sorted set MEMBER is the card fingerprint, not the transaction ID.
    // This means if the same card is used 10 times, it only counts as 1
    // distinct card — which is exactly what we want here.
    // -------------------------------------------------------------------------
    private int scoreDeviceCrossCard(TransactionEvent event) {
        String deviceFp = event.getDeviceFingerprint();
        String cardFp   = event.getCardFingerprint();

        // Guard: if device fingerprint is missing, skip this signal
        if (deviceFp == null || deviceFp.isBlank()) return 0;

        long nowMs    = Instant.now().toEpochMilli();
        long window24h = 86_400_000L; // 24 hours in milliseconds

        String keyDevice = "risk:velocity:device:" + deviceFp + ":24h";

        // Store the CARD FINGERPRINT as the member (not txnId).
        // If this card was already seen on this device today, ZADD will
        // update its score to nowMs — keeping only the most recent timestamp.
        // The count of members = count of distinct cards seen.
        redisTemplate.opsForZSet().add(keyDevice, cardFp, nowMs);

        // Remove cards not seen in the last 24 hours
        redisTemplate.opsForZSet().removeRangeByScore(keyDevice, 0, nowMs - window24h);

        Long distinctCards = redisTemplate.opsForZSet().zCard(keyDevice);
        long deviceCardCount = (distinctCards != null) ? distinctCards : 0L;

        redisTemplate.expire(keyDevice, 25, java.util.concurrent.TimeUnit.HOURS);

        // Score contribution:
        // A device touching 5+ distinct cards in 24h is a serious red flag
        if (deviceCardCount >= 10) return 40;
        if (deviceCardCount >= 5)  return 20;
        if (deviceCardCount >= 3)  return 10;

        return 0;
    }

    // -------------------------------------------------------------------------
    // STAGE 2c — PER-IP CROSS-CARD TRACKING
    // Counts how many DISTINCT card fingerprints this IP has touched in 1h.
    // A residential IP might have a few family members using different cards.
    // A fraud IP will touch dozens of cards in minutes.
    // -------------------------------------------------------------------------
    private int scoreIpCrossCard(TransactionEvent event) {
        String ip     = event.getIpAddress();
        String cardFp = event.getCardFingerprint();

        // Guard: skip if IP is missing or a loopback address (local testing)
        if (ip == null || ip.isBlank() || ip.startsWith("127.")) return 0;

        long nowMs   = Instant.now().toEpochMilli();
        long window1h = 3_600_000L;

        String keyIp = "risk:velocity:ip:" + ip + ":1h";

        // Same pattern as device: store card fingerprint as member
        redisTemplate.opsForZSet().add(keyIp, cardFp, nowMs);
        redisTemplate.opsForZSet().removeRangeByScore(keyIp, 0, nowMs - window1h);

        Long distinctCards = redisTemplate.opsForZSet().zCard(keyIp);
        long ipCardCount = (distinctCards != null) ? distinctCards : 0L;

        redisTemplate.expire(keyIp, 2, java.util.concurrent.TimeUnit.HOURS);

        // Score contribution:
        // An IP touching 5+ distinct cards in 1 hour is highly anomalous
        if (ipCardCount >= 10) return 40;
        if (ipCardCount >= 5)  return 25;
        if (ipCardCount >= 3)  return 10;

        return 0;
    }
}