package com.riskflow.ingestion.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.riskflow.ingestion.model.Transaction;

// Spring Data JPA repository for Transaction persistence.
// JpaRepository gives us save(), findById(), findAll(), delete() for free.
// We only need to declare methods that aren't already provided.
@Repository
public interface TransactionRepository extends JpaRepository<Transaction, Long> {

    // Used by the Risk Scoring Service to look up a transaction by its
    // business-level transactionId (not the auto-generated DB primary key)
    // when updating status after a risk decision is made
    Optional<Transaction> findByTransactionId(String transactionId);
}