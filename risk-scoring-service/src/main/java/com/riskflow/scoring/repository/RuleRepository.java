package com.riskflow.scoring.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.riskflow.scoring.model.Rule;

/**
 * Spring Data repository for loading rules from PostgreSQL.
 *
 * Spring auto-generates the implementation at startup — we only need
 * to declare the method signatures we want.
 *
 * findByEnabledTrue() translates to:
 *   SELECT * FROM rules WHERE enabled = true
 *
 * We only load enabled rules. Disabled rules stay in the database
 * so analysts can re-enable them later, but they never reach the engine.
 */
@Repository
public interface RuleRepository extends JpaRepository<Rule, Long> {

    List<Rule> findByEnabledTrue();
}