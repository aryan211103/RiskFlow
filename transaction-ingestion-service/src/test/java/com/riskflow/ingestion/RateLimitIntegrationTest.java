package com.riskflow.ingestion;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
public class RateLimitIntegrationTest {

    // PostgreSQL container — required for JPA/Hibernate context startup
    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("riskflow")
            .withUsername("riskflow")
            .withPassword("riskflow");

    // Kafka container — required for Spring Kafka autoconfiguration
    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));

    // Redis container — used by the rate limiter
    @Container
    static GenericContainer<?> redis = new GenericContainer<>("redis:7")
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void overrideProps(DynamicPropertyRegistry registry) {
        // PostgreSQL
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);

        // Kafka
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);

        // Redis
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));

        // Low rate limit so we can hit it quickly in the test
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
                    .header("X-Forwarded-For", "192.168.1.100"))
                    .andExpect(status().isOk());
        }

        // 4th request should be rejected with 429
        mockMvc.perform(post("/transactions")
                .contentType(MediaType.APPLICATION_JSON)
                .content(SAMPLE_TRANSACTION)
                .header("X-Forwarded-For", "192.168.1.100"))
                .andExpect(status().isTooManyRequests())
                .andExpect(header().exists("Retry-After"));
    }
}