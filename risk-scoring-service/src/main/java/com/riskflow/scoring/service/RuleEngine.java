package com.riskflow.scoring.service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.stereotype.Service;

import com.riskflow.scoring.dto.TransactionEvent;
import com.riskflow.scoring.model.MatchMode;
import com.riskflow.scoring.model.Rule;
import com.riskflow.scoring.repository.RuleRepository;

import jakarta.annotation.PostConstruct;

/**
 * Stage 3 of the scoring pipeline — the hot-reloadable, composable rule engine.
 *
 * Phase 9 upgrade: two-pass evaluation.
 *
 * PASS 1 — Ungrouped rules (groupName is null):
 *   Evaluated independently, exactly as before Phase 9.
 *   Each matching rule adds its score directly to the total.
 *
 * PASS 2 — Grouped rules (groupName is not null):
 *   Rules sharing the same groupName are collected into a group.
 *   The group's matchMode (ALL or ANY) determines if the group fires.
 *   ALL → every member expression must match (logical AND)
 *   ANY → at least one member expression must match (logical OR)
 *   When the group fires, its score is added ONCE — not once per matching rule.
 *
 * Why two passes instead of one?
 * Ungrouped and grouped rules have fundamentally different semantics.
 * Mixing them in a single loop would require conditional branching that
 * obscures the intent. Separating them makes the logic readable and testable.
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
     * CopyOnWriteArrayList is thread-safe for our pattern:
     * the Kafka consumer thread reads constantly, the hot-reload
     * endpoint writes occasionally. Reads are lock-free.
     */
    private final CopyOnWriteArrayList<Rule> loadedRules = new CopyOnWriteArrayList<>();

    public RuleEngine(RuleRepository ruleRepository) {
        this.ruleRepository = ruleRepository;
    }

    @PostConstruct
    public void loadRules() {
        List<Rule> rules = ruleRepository.findByEnabledTrue();
        loadedRules.clear();
        loadedRules.addAll(rules);
        log.info("Rule engine loaded {} enabled rules", loadedRules.size());
        rules.forEach(r -> log.debug("Loaded rule: name='{}' group='{}' matchMode={}",
            r.getName(), r.getGroupName(), r.getMatchMode()));
    }

    public void reload() {
        log.info("Reloading rules from database...");
        loadRules();
        log.info("Rule reload complete. {} rules active.", loadedRules.size());
    }

    /**
     * Core scoring method — called by TransactionEventConsumer for Stage 3.
     *
     * Splits loaded rules into ungrouped and grouped, runs both passes,
     * and returns the combined score.
     *
     * @param event the transaction to score
     * @return total score contribution from all matching rules and groups
     */
    public int evaluate(TransactionEvent event) {
        int totalScore = 0;

        // -----------------------------------------------------------------------
        // Split rules into two buckets using Java streams.
        //
        // Collectors.partitioningBy splits a stream into two lists based on a
        // boolean predicate:
        //   true  → rules where groupName is null  (ungrouped)
        //   false → rules where groupName is set   (grouped)
        // -----------------------------------------------------------------------
        Map<Boolean, List<Rule>> partitioned = loadedRules.stream()
            .collect(Collectors.partitioningBy(r -> r.getGroupName() == null));

        List<Rule> ungroupedRules = partitioned.get(true);   // groupName == null
        List<Rule> groupedRules   = partitioned.get(false);  // groupName != null

        // -----------------------------------------------------------------------
        // PASS 1 — Ungrouped rules
        // Identical to the pre-Phase 9 evaluation loop.
        // Each matching rule adds its own score to the total independently.
        // -----------------------------------------------------------------------
        for (Rule rule : ungroupedRules) {
            try {
                StandardEvaluationContext context = new StandardEvaluationContext(event);
                Expression expression = parser.parseExpression(rule.getExpression());
                Boolean matched = expression.getValue(context, Boolean.class);

                if (Boolean.TRUE.equals(matched)) {
                    log.info("[UNGROUPED] Rule matched. rule='{}' score={} txnId={}",
                        rule.getName(), rule.getScore(), event.getTransactionId());
                    totalScore += rule.getScore();
                }

            } catch (Exception e) {
                log.error("[UNGROUPED] Rule evaluation failed. rule='{}' expression='{}' error={}",
                    rule.getName(), rule.getExpression(), e.getMessage());
            }
        }

        // -----------------------------------------------------------------------
        // PASS 2 — Grouped rules
        //
        // Step A: Bucket rules by groupName using a LinkedHashMap.
        //         LinkedHashMap preserves insertion order so log output is
        //         deterministic — easier to debug than HashMap.
        //
        // Step B: For each group, evaluate all member expressions.
        //         Apply matchMode logic to decide if the group fires.
        //
        // Step C: If the group fires, find the group's score and add it once.
        //         The score comes from the first member rule with score > 0.
        //         Signal-carrier members have score = 0 by convention.
        // -----------------------------------------------------------------------

        // Step A — group rules by groupName
        // groupingBy returns Map<String, List<Rule>> where key = groupName
        Map<String, List<Rule>> rulesByGroup = groupedRules.stream()
            .collect(Collectors.groupingBy(
                Rule::getGroupName,
                LinkedHashMap::new,        // preserve insertion order
                Collectors.toList()
            ));

        // Step B — evaluate each group
        for (Map.Entry<String, List<Rule>> entry : rulesByGroup.entrySet()) {
            String groupName      = entry.getKey();
            List<Rule> groupRules = entry.getValue();

            // Read matchMode from the first rule in the group.
            // All rules in a group should have the same matchMode — we trust
            // the data is consistent. A future validation step could enforce this.
            MatchMode matchMode = groupRules.get(0).getMatchMode();

            // Evaluate each member expression and collect boolean results
            List<Boolean> results = evaluateGroupMembers(groupRules, event, groupName);

            // Step B: apply matchMode logic
            boolean groupFires;
            if (matchMode == MatchMode.ALL) {
                // ALL → every result must be true (logical AND)
                // allMatch returns true if the stream is empty, so we also
                // check that the group is non-empty.
                groupFires = !results.isEmpty() && results.stream().allMatch(Boolean.TRUE::equals);
            } else {
                // ANY → at least one result must be true (logical OR)
                groupFires = results.stream().anyMatch(Boolean.TRUE::equals);
            }

            // Step C — find the group's score and add it once if the group fires
            if (groupFires) {
                // The group score is on whichever member has score > 0
                int groupScore = groupRules.stream()
                    .mapToInt(Rule::getScore)
                    .filter(s -> s > 0)
                    .findFirst()
                    .orElse(0);   // fallback: if all members have score=0, add nothing

                log.info("[GROUP FIRED] group='{}' matchMode={} score={} txnId={}",
                    groupName, matchMode, groupScore, event.getTransactionId());

                totalScore += groupScore;
            } else {
                log.info("[GROUP SKIPPED] group='{}' matchMode={} results={} txnId={}",
                    groupName, matchMode, results, event.getTransactionId());
            }
        }

        log.info("Rule engine total score. txnId={} score={}", event.getTransactionId(), totalScore);
        return totalScore;
    }

    /**
     * Evaluates all member expressions in a rule group and returns a list
     * of boolean results in the same order as the input rules.
     *
     * Kept as a separate method so the main evaluate() loop stays readable.
     * Errors in individual member expressions are logged and treated as
     * non-matches (false) so one bad rule does not poison the whole group.
     *
     * @param groupRules  all rules belonging to the group
     * @param event       the transaction being scored
     * @param groupName   used only for log messages
     * @return            list of booleans, one per rule, in input order
     */
    private List<Boolean> evaluateGroupMembers(List<Rule> groupRules,
                                                TransactionEvent event,
                                                String groupName) {
        List<Boolean> results = new ArrayList<>();

        for (Rule rule : groupRules) {
            // Score-bearer rules use expression "true" — they always match.
            // Their purpose is to carry the group score, not to filter.
            // We skip them from the match-logic results so they do not
            // artificially inflate the match count for ANY groups.
            if (rule.getScore() > 0) {
                continue;
            }

            try {
                StandardEvaluationContext context = new StandardEvaluationContext(event);
                Expression expression = parser.parseExpression(rule.getExpression());
                Boolean matched = expression.getValue(context, Boolean.class);
                boolean result = Boolean.TRUE.equals(matched);

                log.debug("[GROUP MEMBER] group='{}' rule='{}' matched={} txnId={}",
                    groupName, rule.getName(), result, event.getTransactionId());

                results.add(result);

            } catch (Exception e) {
                log.error("[GROUP MEMBER] Evaluation failed. group='{}' rule='{}' error={}",
                    groupName, rule.getName(), e.getMessage());
                // Treat evaluation errors as non-matches so one broken rule
                // does not prevent the rest of the group from being evaluated.
                results.add(false);
            }
        }

        return results;
    }
}