package com.riskflow.scoring.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.riskflow.scoring.model.RiskDecision;

/**
 * Spring Data JPA repository for RiskDecision entities.
 *
 * By extending JpaRepository<RiskDecision, Long>, we get these methods
 * for free — no implementation needed:
 *
 *   save(entity)           → INSERT or UPDATE
 *   findById(id)           → SELECT WHERE id = ?
 *   findAll()              → SELECT * (avoid in production — use pagination)
 *   deleteById(id)         → DELETE WHERE id = ?
 *   existsById(id)         → SELECT COUNT WHERE id = ?
 *   count()                → SELECT COUNT(*)
 *
 * Spring Data also supports derived query methods — methods whose names
 * encode the query. For example, if we added:
 *
 *   List<RiskDecision> findByTransactionId(String transactionId);
 *
 * Spring would generate: SELECT * FROM risk_decisions WHERE transaction_id = ?
 * No SQL needed. The method name IS the query.
 *
 * For Phase 4b we only need save(). The other methods will become useful
 * in Phase 5 when the admin endpoints need to query decisions.
 *
 * @Repository is optional here (JpaRepository sub-interfaces are detected
 * automatically) but included for clarity — it signals intent and enables
 * Spring's persistence exception translation.
 */
@Repository
public interface RiskDecisionRepository extends JpaRepository<RiskDecision, Long> {
    // No methods needed yet. save() from JpaRepository is sufficient for Phase 4b.
}