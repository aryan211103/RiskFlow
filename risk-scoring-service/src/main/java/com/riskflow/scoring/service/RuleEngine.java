package com.riskflow.scoring.service;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.stereotype.Service;

import com.riskflow.scoring.dto.TransactionEvent;
import com.riskflow.scoring.model.Rule;
import com.riskflow.scoring.repository.RuleRepository;

import jakarta.annotation.PostConstruct;

/**
 * Stage 3 of the scoring pipeline — the hot-reloadable rule engine.
 *
 * Responsibilities:
 *   1. Load enabled rules from PostgreSQL at startup
 *   2. Evaluate each rule's SpEL expression against the transaction
 *   3. Accumulate score contributions from all matching rules
 *   4. Expose a reload() method so rules can be refreshed without restart
 *
 * Why accumulate instead of short-circuit?
 * Each rule captures a weak signal. Fraud decisions require the combination
 * of multiple weak signals. Short-circuiting would miss the full picture.
 */
@Service
public class RuleEngine {

    private static final Logger log = LoggerFactory.getLogger(RuleEngine.class);

    private final RuleRepository ruleRepository;

    /**
     * SpEL expression parser — stateless and thread-safe.
     * Parses a string like "amount > 500" into an Expression object
     * that can be evaluated against any context object.
     */
    private final ExpressionParser parser = new SpelExpressionParser();

    /**
     * In-memory cache of loaded rules.
     *
     * Why CopyOnWriteArrayList instead of ArrayList?
     * The Kafka consumer thread reads this list constantly.
     * The hot-reload endpoint writes to it occasionally.
     * CopyOnWriteArrayList is thread-safe for this pattern:
     * reads are lock-free, writes create a new copy of the array.
     * ArrayList is not thread-safe — concurrent read + write would
     * cause a ConcurrentModificationException.
     */
    private final CopyOnWriteArrayList<Rule> loadedRules = new CopyOnWriteArrayList<>();

    public RuleEngine(RuleRepository ruleRepository) {
        this.ruleRepository = ruleRepository;
    }

    /**
     * @PostConstruct runs once after Spring has finished wiring all
     * dependencies into this bean — but before the application starts
     * serving requests or consuming Kafka messages.
     *
     * This is the right place to load rules because:
     *   1. ruleRepository is guaranteed to be injected by this point
     *   2. No transactions will be scored before rules are loaded
     */
    @PostConstruct
    public void loadRules() {
        List<Rule> rules = ruleRepository.findByEnabledTrue();
        loadedRules.clear();
        loadedRules.addAll(rules);
        log.info("Rule engine loaded {} enabled rules", loadedRules.size());
        rules.forEach(r -> log.debug("Loaded rule: {}", r));
    }

    /**
     * Hot-reload endpoint — called by the admin REST controller.
     *
     * Fetches the latest enabled rules from PostgreSQL and replaces
     * the in-memory list. The Kafka consumer thread continues scoring
     * transactions during the reload — it just uses the old list until
     * the swap completes, which is atomic in CopyOnWriteArrayList.
     *
     * No restart needed. No downtime.
     */
    public void reload() {
        log.info("Reloading rules from database...");
        loadRules();
        log.info("Rule reload complete. {} rules active.", loadedRules.size());
    }

    /**
     * Core scoring method — called by TransactionEventConsumer for Stage 3.
     *
     * Evaluates every loaded rule against the transaction and accumulates
     * score contributions from all matching rules.
     *
     * @param event the transaction to score
     * @return total score contribution from all matching rules
     */
    public int evaluate(TransactionEvent event) {
        int totalScore = 0;

        for (Rule rule : loadedRules) {
            try {
                /**
                 * StandardEvaluationContext wraps the transaction event as
                 * the "root object" for SpEL evaluation.
                 *
                 * When SpEL sees "amount > 500", it looks for a field or
                 * getter called "amount" on the root object — which is our
                 * TransactionEvent. It calls getAmount() and compares to 500.
                 *
                 * The context is created fresh per rule to avoid any state
                 * leaking between evaluations.
                 */
                StandardEvaluationContext context = new StandardEvaluationContext(event);

                // Parse the expression string into an evaluatable Expression object
                Expression expression = parser.parseExpression(rule.getExpression());

                // Evaluate
                Boolean matched = expression.getValue(context, Boolean.class);

                if (Boolean.TRUE.equals(matched)) {
                    log.info("Rule matched. rule='{}' score={} txnId={}",
                        rule.getName(), rule.getScore(), event.getTransactionId());
                    totalScore += rule.getScore();
                }

            } catch (Exception e) {
                log.error("Rule evaluation failed. rule='{}' expression='{}' error={}",
                    rule.getName(), rule.getExpression(), e.getMessage());
            }
        }

        log.info("Rule engine total score. txnId={} score={}", event.getTransactionId(), totalScore);
        return totalScore;
    }

}