package com.riskflow.scoring.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.riskflow.scoring.model.RiskDecision;

@Repository
public interface RiskDecisionRepository extends JpaRepository<RiskDecision, Long> {

    // Used by the integration test to verify a decision was saved.
    // Spring Data generates: SELECT * FROM risk_decisions WHERE transaction_id = ?
    Optional<RiskDecision> findByTransactionId(String transactionId);

    // Used by the idempotency integration test to count how many times
    // a transaction was processed. Should always return 1, never 2.
    // Spring Data generates: SELECT COUNT(*) FROM risk_decisions WHERE transaction_id = ?
    long countByTransactionId(String transactionId);
}