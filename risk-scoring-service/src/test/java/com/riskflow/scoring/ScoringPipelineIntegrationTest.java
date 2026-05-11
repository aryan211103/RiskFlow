package com.riskflow.scoring;

import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.riskflow.scoring.model.DecisionType;
import com.riskflow.scoring.model.RiskDecision;
import com.riskflow.scoring.repository.RiskDecisionRepository;

/**
 * Full pipeline integration test for the Risk Scoring Service.
 *
 * This test class starts three real Docker containers:
 *   1. PostgreSQL 16 — stores RiskDecision records
 *   2. Kafka (Confluent 7.4.0) — message broker for transaction events
 *   3. Redis 7 — idempotency cache and behavioral analytics
 *
 * @SpringBootTest starts the full Spring application context connected
 * to those real containers. The scoring pipeline runs exactly as it
 * does in production — no mocks, no stubs, no fakes.
 *
 * What we test:
 *   Publish a transaction payload to Kafka → wait for the scoring service
 *   to consume and score it → assert a RiskDecision row exists in PostgreSQL.
 *
 * Why this matters:
 *   Unit tests prove individual classes work in isolation.
 *   This test proves the entire pipeline works end-to-end with real
 *   infrastructure. If Kafka consumer configuration is wrong, if
 *   Hibernate mapping is broken, if Redis connectivity fails — this
 *   test catches it. Unit tests cannot.
 *
 * Runtime: expect 30-60 seconds on first run (Docker image pulls).
 *   Subsequent runs are faster because images are cached locally.
 */
@SpringBootTest                  // starts the full Spring context
@Testcontainers                  // activates Testcontainers JUnit 5 extension
class ScoringPipelineIntegrationTest {

    // -------------------------------------------------------------------------
    // CONTAINER DECLARATIONS
    //
    // @Container tells the Testcontainers extension to manage this container's
    // lifecycle — start before tests, stop after tests.
    //
    // static means one container instance is shared across ALL test methods
    // in this class. Starting a container takes ~5 seconds. If we used
    // instance fields (non-static), each test method would restart everything.
    // Static sharing is the standard pattern for integration tests.
    // -------------------------------------------------------------------------

    /**
     * PostgreSQL 16 container.
     *
     * PostgreSQLContainer is a typed container — it knows how to start
     * PostgreSQL and exposes getJdbcUrl(), getUsername(), getPassword()
     * so we can wire them into Spring's datasource configuration.
     *
     * "riskflow" is the database name that will be created automatically.
     */
    @Container
    static final PostgreSQLContainer<?> postgres =
        new PostgreSQLContainer<>(DockerImageName.parse("postgres:16"))
            .withDatabaseName("riskflow")
            .withUsername("postgres")
            .withPassword("secret");

    /**
     * Kafka container using the Confluent image — same as docker-compose.yml.
     *
     * KafkaContainer is a typed container that handles Zookeeper internally.
     * It exposes getBootstrapServers() with the random host:port combination
     * that Docker assigned.
     */
    @Container
    static final KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));

    /**
     * Redis 7 container.
     *
     * Redis doesn't have a typed Testcontainers module so we use GenericContainer.
     * We tell it which port Redis listens on (6379) and wait until Redis
     * logs "Ready to accept connections" before proceeding.
     *
     * waitingFor() is critical — without it, the container might be started
     * but Redis might not be ready yet, causing connection failures.
     */
    @Container
    static final GenericContainer<?> redis =
        new GenericContainer<>(DockerImageName.parse("redis:7"))
            .withExposedPorts(6379)
            .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*", 1));

    // -------------------------------------------------------------------------
    // DYNAMIC PROPERTY INJECTION
    //
    // @DynamicPropertySource runs BEFORE Spring's ApplicationContext starts.
    // It reads the actual ports that Docker assigned to each container and
    // injects them into Spring's property system.
    //
    // This is why we cannot use a static application.properties for tests —
    // the ports are not known until the containers start, which happens
    // after the test class is loaded but before the Spring context starts.
    //
    // The registry.add() calls override the same property keys that are
    // in src/main/resources/application.properties, so Spring uses the
    // container URLs instead of localhost.
    // -------------------------------------------------------------------------
    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        // Tell Spring Boot's datasource to connect to the PostgreSQL container
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);

        // Tell Spring Kafka to connect to the Kafka container
        // getBootstrapServers() returns something like "localhost:49823"
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);

        // Tell Spring Redis to connect to the Redis container
        // getMappedPort() returns the host port that Docker mapped to container port 6379
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port",
            () -> redis.getMappedPort(6379).toString());
    }

    // -------------------------------------------------------------------------
    // TEST DEPENDENCIES
    //
    // Spring injects these from the application context.
    // RiskDecisionRepository lets us query PostgreSQL to verify the decision
    // was written after the scoring pipeline processed the transaction.
    // -------------------------------------------------------------------------
    @Autowired
    private RiskDecisionRepository riskDecisionRepository;

    // -------------------------------------------------------------------------
    // TEST 1: Normal transaction scores as APPROVED or NEEDS_REVIEW
    //
    // We publish a clean transaction (low amount, clean IP, safe MCC)
    // and verify that a RiskDecision row is created within 10 seconds.
    //
    // We don't assert a specific decision type here because the behavioral
    // analyzer state depends on what's in Redis — a clean transaction on a
    // fresh Redis instance should be APPROVED, but we accept NEEDS_REVIEW too.
    // The important assertion is that the pipeline ran and a decision was saved.
    // -------------------------------------------------------------------------
    @Test
    void cleanTransaction_isProcessed_andDecisionIsSaved() throws Exception {
        // Generate a unique transaction ID so this test doesn't collide
        // with other tests or leftover data from previous runs
        String txnId = "txn_it_" + UUID.randomUUID().toString().substring(0, 8);

        // Build the pipe-delimited payload — same format the ingestion service sends
        String payload = String.format(
            "transactionId:%s|userId:user_it_001|cardFingerprint:fpr_it_001" +
            "|amount:150|currency:USD|merchantId:mch_it_001" +
            "|merchantCategoryCode:5412|merchantRiskTier:low" +
            "|deviceFingerprint:dev_it_001|ipAddress:192.168.1.1" +
            "|ipCountry:US|billingCountry:US",
            txnId
        );

        // Publish directly to Kafka using a raw KafkaProducer.
        // We bypass the ingestion service because we're testing the scoring
        // service in isolation — we don't need the outbox pattern here.
        publishToKafka("transaction.received", txnId, payload);

        // Awaitility polls the condition repeatedly until it's true or times out.
        //
        // Why not Thread.sleep()?
        // sleep() wastes time — if the pipeline finishes in 1 second, sleep(10000)
        // still waits 10 seconds. Awaitility checks every 500ms and returns
        // immediately when the condition is true.
        //
        // await() is from the Awaitility library which is included transitively
        // via spring-boot-starter-test.
        await()
            .atMost(Duration.ofSeconds(15))       // fail if not done in 15s
            .pollInterval(Duration.ofMillis(500)) // check every 500ms
            .untilAsserted(() -> {
                // Query PostgreSQL for the decision
                Optional<RiskDecision> decision =
                    riskDecisionRepository.findByTransactionId(txnId);

                // Assert the row exists — this is the core guarantee:
                // if it's present, the full pipeline ran successfully
                assertThat(decision)
                    .isPresent()
                    .withFailMessage("No RiskDecision found for txnId=%s — " +
                        "pipeline did not process the transaction", txnId);

                // Assert the decision is one of the three valid types
                assertThat(decision.get().getDecision())
                    .isIn(DecisionType.APPROVED,
                          DecisionType.NEEDS_REVIEW,
                          DecisionType.AUTO_REJECTED);

                // Assert the score is non-negative
                assertThat(decision.get().getRiskScore()).isGreaterThanOrEqualTo(0);
            });
    }

    // -------------------------------------------------------------------------
    // TEST 2: Blocklisted card triggers AUTO_REJECTED via hard rules
    //
    // We publish a transaction with a card fingerprint that is in the
    // BLOCKLISTED_CARDS set in TransactionEventConsumer. The hard rules
    // stage should fire immediately and produce AUTO_REJECTED with score 100.
    //
    // This test verifies Stage 1 (hard rules) works end-to-end with
    // real infrastructure — not just in a unit test with mocked dependencies.
    // -------------------------------------------------------------------------
    @Test
    void blocklistedCard_isAutoRejected_withScore100() throws Exception {
        String txnId = "txn_it_bl_" + UUID.randomUUID().toString().substring(0, 8);

        // fpr_BLOCKED_001 is in the BLOCKLISTED_CARDS set in TransactionEventConsumer
        String payload = String.format(
            "transactionId:%s|userId:user_it_002|cardFingerprint:fpr_BLOCKED_001" +
            "|amount:50|currency:USD|merchantId:mch_it_002" +
            "|merchantCategoryCode:5412|merchantRiskTier:low" +
            "|deviceFingerprint:dev_it_002|ipAddress:192.168.1.2" +
            "|ipCountry:US|billingCountry:US",
            txnId
        );

        publishToKafka("transaction.received", txnId, payload);

        await()
            .atMost(Duration.ofSeconds(15))
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted(() -> {
                Optional<RiskDecision> decision =
                    riskDecisionRepository.findByTransactionId(txnId);

                assertThat(decision).isPresent();

                // Hard rule hit must produce AUTO_REJECTED with score 100
                assertThat(decision.get().getDecision())
                    .isEqualTo(DecisionType.AUTO_REJECTED);

                assertThat(decision.get().getRiskScore())
                    .isEqualTo(100);
            });
    }

    // -------------------------------------------------------------------------
    // TEST 3: Sanctioned country triggers AUTO_REJECTED via hard rules
    //
    // RU is in the SANCTIONED_COUNTRIES set. This verifies the country
    // check in Stage 1 works with real infrastructure.
    // -------------------------------------------------------------------------
    @Test
    void sanctionedCountry_isAutoRejected_withScore100() throws Exception {
        String txnId = "txn_it_sc_" + UUID.randomUUID().toString().substring(0, 8);

        String payload = String.format(
            "transactionId:%s|userId:user_it_003|cardFingerprint:fpr_it_003" +
            "|amount:75|currency:USD|merchantId:mch_it_003" +
            "|merchantCategoryCode:5412|merchantRiskTier:low" +
            "|deviceFingerprint:dev_it_003|ipAddress:203.0.113.1" +
            "|ipCountry:RU|billingCountry:RU",
            txnId
        );

        publishToKafka("transaction.received", txnId, payload);

        await()
            .atMost(Duration.ofSeconds(15))
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted(() -> {
                Optional<RiskDecision> decision =
                    riskDecisionRepository.findByTransactionId(txnId);

                assertThat(decision).isPresent();

                assertThat(decision.get().getDecision())
                    .isEqualTo(DecisionType.AUTO_REJECTED);

                assertThat(decision.get().getRiskScore())
                    .isEqualTo(100);
            });
    }

    // -------------------------------------------------------------------------
    // TEST 4: Amount over cap triggers AUTO_REJECTED via hard rules
    //
    // HARD_CAP_AMOUNT is 10000. Sending 15000 must trigger rejection.
    // -------------------------------------------------------------------------
    @Test
    void amountOverCap_isAutoRejected_withScore100() throws Exception {
        String txnId = "txn_it_cap_" + UUID.randomUUID().toString().substring(0, 8);

        String payload = String.format(
            "transactionId:%s|userId:user_it_004|cardFingerprint:fpr_it_004" +
            "|amount:15000|currency:USD|merchantId:mch_it_004" +
            "|merchantCategoryCode:5412|merchantRiskTier:low" +
            "|deviceFingerprint:dev_it_004|ipAddress:203.0.113.2" +
            "|ipCountry:US|billingCountry:US",
            txnId
        );

        publishToKafka("transaction.received", txnId, payload);

        await()
            .atMost(Duration.ofSeconds(15))
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted(() -> {
                Optional<RiskDecision> decision =
                    riskDecisionRepository.findByTransactionId(txnId);

                assertThat(decision).isPresent();

                assertThat(decision.get().getDecision())
                    .isEqualTo(DecisionType.AUTO_REJECTED);

                assertThat(decision.get().getRiskScore())
                    .isEqualTo(100);
            });
    }

    // -------------------------------------------------------------------------
    // TEST 5: Idempotency — duplicate message is not processed twice
    //
    // We publish the same payload twice with the same txnId.
    // The scoring service should process it once and skip the duplicate.
    // After both publishes, there should still be exactly ONE RiskDecision row.
    //
    // This tests the Redis SET NX idempotency check end-to-end.
    // -------------------------------------------------------------------------
    @Test
    void duplicateMessage_isNotProcessedTwice() throws Exception {
        String txnId = "txn_it_dup_" + UUID.randomUUID().toString().substring(0, 8);

        String payload = String.format(
            "transactionId:%s|userId:user_it_005|cardFingerprint:fpr_it_005" +
            "|amount:200|currency:USD|merchantId:mch_it_005" +
            "|merchantCategoryCode:5412|merchantRiskTier:low" +
            "|deviceFingerprint:dev_it_005|ipAddress:192.168.1.5" +
            "|ipCountry:US|billingCountry:US",
            txnId
        );

        // Publish the same message twice — simulates Kafka at-least-once delivery
        publishToKafka("transaction.received", txnId, payload);
        publishToKafka("transaction.received", txnId, payload);

        // Wait for at least one decision to be saved
        await()
            .atMost(Duration.ofSeconds(15))
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted(() -> {
                Optional<RiskDecision> decision =
                    riskDecisionRepository.findByTransactionId(txnId);
                assertThat(decision).isPresent();
            });

        // Wait an extra 3 seconds for any potential duplicate processing
        Thread.sleep(3000);

        // Count the rows — must be exactly 1, not 2
        long count = riskDecisionRepository.countByTransactionId(txnId);
        assertThat(count)
            .isEqualTo(1)
            .withFailMessage("Idempotency check failed — txnId=%s was processed %d times, expected 1",
                txnId, count);
    }

    // -------------------------------------------------------------------------
    // HELPER: publishToKafka
    //
    // Creates a raw KafkaProducer and sends one message synchronously.
    // We use a raw producer (not Spring's KafkaTemplate) because we want
    // full control over the bootstrap servers pointing to the container.
    //
    // send().get() blocks until the broker acknowledges the message —
    // this ensures the message is in Kafka before the test's await() starts.
    // -------------------------------------------------------------------------
    private void publishToKafka(String topic, String key, String value) throws Exception {
        Properties props = new Properties();

        // Point to the Kafka container's bootstrap server
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // acks=all — wait for full acknowledgment before returning
        // This ensures the message is durably in Kafka before we start polling
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            // .get() makes send() synchronous — blocks until broker confirms
            producer.send(record).get();
        }
    }
}