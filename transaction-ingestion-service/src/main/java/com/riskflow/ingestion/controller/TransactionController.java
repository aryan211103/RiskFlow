package com.riskflow.ingestion.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.riskflow.ingestion.dto.CreateTransactionRequest;
import com.riskflow.ingestion.model.Transaction;
import com.riskflow.ingestion.service.TransactionService;

// REST controller exposing the transaction ingestion endpoint.
// This is the entry point for all incoming transaction events —
// either from real clients or from the synthetic transaction generator (Phase 8).
@RestController
@RequestMapping("/transactions")
public class TransactionController {

    private final TransactionService transactionService;

    public TransactionController(TransactionService transactionService) {
        this.transactionService = transactionService;
    }

    // POST /transactions
    // Accepts a transaction event, persists it, publishes to Kafka, returns 200.
    // The caller gets an immediate response — risk scoring happens asynchronously.
    // The transaction's status will transition from PENDING to APPROVED/NEEDS_REVIEW/AUTO_REJECTED
    // once the Risk Scoring Service processes the corresponding Kafka event.
    @PostMapping
    public ResponseEntity<Transaction> createTransaction(@RequestBody CreateTransactionRequest request) {
        Transaction transaction = transactionService.createTransaction(request);
        return ResponseEntity.ok(transaction);
    }
}