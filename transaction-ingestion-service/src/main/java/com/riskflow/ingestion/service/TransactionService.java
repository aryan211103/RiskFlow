package com.riskflow.ingestion.service;

import java.util.UUID;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.riskflow.ingestion.dto.CreateTransactionRequest;
import com.riskflow.ingestion.model.OutboxEvent;
import com.riskflow.ingestion.model.Transaction;
import com.riskflow.ingestion.repository.OutboxEventRepository;
import com.riskflow.ingestion.repository.TransactionRepository;

// Business logic layer for transaction ingestion.
// Responsibilities:
//   1. Map the incoming DTO to a Transaction entity
//   2. In a single atomic PostgreSQL transaction:
//      a. Persist the Transaction with status=PENDING
//      b. Persist an OutboxEvent with the Kafka payload
//   3. The OutboxPoller (separate class) handles publishing to Kafka
//
// The KafkaTemplate has been removed from this class entirely.
// This service no longer knows Kafka exists — that concern belongs
// to the OutboxPoller.
@Service
public class TransactionService {

    private final TransactionRepository transactionRepository;
    private final OutboxEventRepository outboxEventRepository;

    public TransactionService(TransactionRepository transactionRepository,
                              OutboxEventRepository outboxEventRepository) {
        this.transactionRepository = transactionRepository;
        this.outboxEventRepository = outboxEventRepository;
    }

    // @Transactional tells Spring to wrap this entire method in a single
    // PostgreSQL transaction. Both the transaction save and the outbox save
    // will either both commit or both roll back. This is the core guarantee
    // of the outbox pattern — no dual-write problem.
    @Transactional
    public Transaction createTransaction(CreateTransactionRequest request) {

        // Build the Transaction entity from the incoming request
        Transaction transaction = new Transaction();

        // Generate a unique business-level transaction ID.
        // UUID ensures global uniqueness — this is also the idempotency key
        // used by the Risk Scoring Service to detect duplicate Kafka deliveries.
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

        // First write: persist the transaction.
        // @PrePersist sets status=PENDING and createdAt automatically.
        Transaction saved = transactionRepository.save(transaction);

        // Build the Kafka payload string — same format as before.
        // We build it here so the OutboxPoller does not need to know
        // anything about the Transaction model. The poller just reads
        // the payload string and sends it. Clean separation of concerns.
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

        // Second write: persist the outbox record in the SAME transaction.
        // If this save fails, the transaction save above is also rolled back.
        // If the service crashes after both saves, the outbox record survives
        // and the poller will publish it on recovery.
        OutboxEvent outboxEvent = new OutboxEvent();
        outboxEvent.setTransactionId(saved.getTransactionId());
        outboxEvent.setPayload(kafkaPayload);
        // status defaults to PENDING — no need to set it manually
        outboxEventRepository.save(outboxEvent);

        // Return the saved transaction to the controller.
        // We do NOT publish to Kafka here — that is the poller's job.
        return saved;
    }
}