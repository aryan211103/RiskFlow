package com.riskflow.ingestion.service;

import java.util.UUID;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.riskflow.ingestion.dto.CreateTransactionRequest;
import com.riskflow.ingestion.model.Transaction;
import com.riskflow.ingestion.repository.TransactionRepository;

// Business logic layer for transaction ingestion.
// Responsibilities:
//   1. Map the incoming DTO to a Transaction entity
//   2. Persist the transaction to PostgreSQL with status=PENDING
//   3. Publish a Kafka event to topic "transaction.received"
//
// Note: In Phase 6 (Tier 2), steps 2 and 3 will be replaced by the
// transactional outbox pattern — a single DB transaction that writes both
// the transaction row and an outbox_events row, with a background relay
// handling the Kafka publish. For now we use the naive dual-write approach.
@Service
public class TransactionService {

    private final TransactionRepository transactionRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    // Kafka topic name — matches what the Risk Scoring Service listens on
    private static final String TOPIC = "transaction.received";

    public TransactionService(TransactionRepository transactionRepository,
                              KafkaTemplate<String, String> kafkaTemplate) {
        this.transactionRepository = transactionRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    public Transaction createTransaction(CreateTransactionRequest request) {

        // Build the Transaction entity from the incoming request
        Transaction transaction = new Transaction();

        // Generate a unique business-level transaction ID
        // UUID ensures global uniqueness — this is also the idempotency key
        // used by the Risk Scoring Service to detect duplicate Kafka deliveries
        transaction.setTransactionId("txn_" + UUID.randomUUID().toString().substring(0, 8));

        transaction.setUserId(request.getUserId());
        transaction.setCardFingerprint(request.getCardFingerprint());
        transaction.setAmount(request.getAmount());
        transaction.setCurrency(request.getCurrency());
        transaction.setMerchantId(request.getMerchantId());
        transaction.setMerchantCategoryCode(request.getMerchantCategoryCode());
        transaction.setMerchantRiskTier(request.getMerchantRiskTier());
        transaction.setDeviceFingerprint(request.getDeviceFingerprint());
        transaction.setIpAddress(request.getIpAddress());
        transaction.setIpCountry(request.getIpCountry());
        transaction.setBillingCountry(request.getBillingCountry());
        transaction.setUserAgent(request.getUserAgent());

        // Persist to PostgreSQL — @PrePersist sets status=PENDING and createdAt automatically
        Transaction saved = transactionRepository.save(transaction);

        // Build the Kafka message payload — pipe-delimited key:value pairs
        // The Risk Scoring Service parses this format when it consumes the event
        // In Phase 6 this naive string format will be replaced with Avro schemas
        String kafkaPayload = String.format(
            "transactionId:%s|userId:%s|cardFingerprint:%s|amount:%d|currency:%s|" +
            "merchantId:%s|merchantCategoryCode:%s|merchantRiskTier:%s|" +
            "deviceFingerprint:%s|ipAddress:%s|ipCountry:%s|billingCountry:%s",
            saved.getTransactionId(),
            saved.getUserId(),
            saved.getCardFingerprint(),
            saved.getAmount(),
            saved.getCurrency(),
            saved.getMerchantId(),
            saved.getMerchantCategoryCode(),
            saved.getMerchantRiskTier(),
            saved.getDeviceFingerprint(),
            saved.getIpAddress(),
            saved.getIpCountry(),
            saved.getBillingCountry()
        );

        // Publish to Kafka — fire and forget for now
        // The Risk Scoring Service will pick this up asynchronously
        kafkaTemplate.send(TOPIC, saved.getTransactionId(), kafkaPayload);

        return saved;
    }
}