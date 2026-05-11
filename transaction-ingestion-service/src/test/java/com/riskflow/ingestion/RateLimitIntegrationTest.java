package com.riskflow.ingestion;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
public class RateLimitIntegrationTest {

    @Container
    static GenericContainer<?> redis = new GenericContainer<>("redis:7")
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void overrideProps(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
        // Set a very low limit so we can hit it in the test quickly
        registry.add("rate.limit.requests", () -> "3");
        registry.add("rate.limit.window.seconds", () -> "60");
    }

    @Autowired
    private MockMvc mockMvc;

    private static final String SAMPLE_TRANSACTION = """
        {
            "userId": "user-test-1",
            "cardFingerprint": "card-abc123",
            "amount": 5000,
            "currency": "USD",
            "merchantId": "merchant-1",
            "merchantCategoryCode": "5411",
            "merchantRiskTier": "LOW",
            "deviceFingerprint": "device-xyz",
            "ipAddress": "192.168.1.100",
            "ipCountry": "US",
            "billingCountry": "US",
            "userAgent": "Mozilla/5.0"
        }
        """;

    @Test
    void shouldReturn429AfterLimitExceeded() throws Exception {
        // First 3 requests should pass (limit is 3)
        for (int i = 0; i < 3; i++) {
            mockMvc.perform(post("/transactions")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(SAMPLE_TRANSACTION)
                    .header("X-Forwarded-For", "192.168.1.100")) // simulate same IP
                    .andExpect(status().isOk());
        }

        // 4th request should be rejected
        mockMvc.perform(post("/transactions")
                .contentType(MediaType.APPLICATION_JSON)
                .content(SAMPLE_TRANSACTION)
                .header("X-Forwarded-For", "192.168.1.100"))
                .andExpect(status().isTooManyRequests())
                .andExpect(header().exists("Retry-After"));
    }
}