package com.riskflow.ingestion.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import com.riskflow.ingestion.interceptor.RateLimitInterceptor;

@Configuration
public class RateLimitConfig implements WebMvcConfigurer {

    private final RateLimitInterceptor rateLimitInterceptor;

    public RateLimitConfig(RateLimitInterceptor rateLimitInterceptor) {
        this.rateLimitInterceptor = rateLimitInterceptor;
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        // Apply rate limiting only to the transaction submission endpoint
        // Not to health checks or actuator endpoints — those shouldn't be rate limited
        registry.addInterceptor(rateLimitInterceptor)
                .addPathPatterns("/transactions/**");
    }
}