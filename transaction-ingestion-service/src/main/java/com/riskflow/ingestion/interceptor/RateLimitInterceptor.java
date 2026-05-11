package com.riskflow.ingestion.interceptor;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import java.util.concurrent.TimeUnit;

@Component
public class RateLimitInterceptor implements HandlerInterceptor {

    private final StringRedisTemplate redis;

    // Max requests allowed per window — loaded from application.properties
    @Value("${rate.limit.requests:100}")
    private int maxRequests;

    // Window size in seconds — loaded from application.properties
    @Value("${rate.limit.window.seconds:60}")
    private int windowSeconds;

    public RateLimitInterceptor(StringRedisTemplate redis) {
        this.redis = redis;
    }

    @Override
    public boolean preHandle(HttpServletRequest request,
                             HttpServletResponse response,
                             Object handler) throws Exception {

        // Extract identifier: prefer X-User-Id header, fall back to IP address
        // In production you'd always use an authenticated user ID — IP can be spoofed
        // or shared (NAT). For this project, IP is fine for demo purposes.
        String identifier = request.getHeader("X-User-Id");
        if (identifier == null || identifier.isBlank()) {
            identifier = request.getRemoteAddr();
        }

        // Build a time bucket that changes every windowSeconds
        // e.g. if windowSeconds=60, bucket changes every minute
        // This is fixed-window rate limiting — simple and explainable
        long windowBucket = System.currentTimeMillis() / 1000 / windowSeconds;

        // Redis key uniquely identifies: who + which time window
        String redisKey = "rate:" + identifier + ":" + windowBucket;

        // INCR atomically increments the counter and returns the new value
        // If the key doesn't exist yet, Redis treats it as 0 before incrementing
        Long currentCount = redis.opsForValue().increment(redisKey);

        // On the first request in this window, set the TTL
        // We set it to 2x window size to ensure the key doesn't expire mid-window
        // and to give some buffer for clock skew
        if (currentCount != null && currentCount == 1) {
            redis.expire(redisKey, windowSeconds * 2L, TimeUnit.SECONDS);
        }

        // If over the limit, reject with 429
        if (currentCount != null && currentCount > maxRequests) {
            response.setStatus(HttpStatus.TOO_MANY_REQUESTS.value());

            // Retry-After header tells the client how long to wait
            // They should wait until the current window bucket expires
            long secondsUntilReset = windowSeconds - (System.currentTimeMillis() / 1000 % windowSeconds);
            response.setHeader("Retry-After", String.valueOf(secondsUntilReset));
            response.setHeader("X-RateLimit-Limit", String.valueOf(maxRequests));
            response.setHeader("X-RateLimit-Remaining", "0");
            response.getWriter().write("{\"error\": \"Rate limit exceeded\", \"retryAfter\": " + secondsUntilReset + "}");
            return false; // stops the request from reaching the controller
        }

        // Add remaining count header for transparency — useful for clients
        if (currentCount != null) {
            response.setHeader("X-RateLimit-Limit", String.valueOf(maxRequests));
            response.setHeader("X-RateLimit-Remaining", String.valueOf(Math.max(0, maxRequests - currentCount)));
        }

        return true; // proceed to controller
    }
}