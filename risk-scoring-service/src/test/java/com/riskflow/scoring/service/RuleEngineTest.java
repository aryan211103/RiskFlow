package com.riskflow.scoring.service;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import static org.mockito.Mockito.when;
import org.mockito.junit.jupiter.MockitoExtension;

import com.riskflow.scoring.dto.TransactionEvent;
import com.riskflow.scoring.model.MatchMode;
import com.riskflow.scoring.model.Rule;
import com.riskflow.scoring.repository.RuleRepository;

@ExtendWith(MockitoExtension.class)
class RuleEngineTest {

    @Mock
    private RuleRepository ruleRepository;

    private RuleEngine ruleEngine;

    @BeforeEach
    void setUp() {
        ruleEngine = new RuleEngine(ruleRepository);
    }

    // -------------------------------------------------------------------------
    // Helper: builds a TransactionEvent with controllable fields.
    // -------------------------------------------------------------------------
    private TransactionEvent buildEvent(double amount, String mcc,
                                         String ipCountry, String billingCountry) {
        String payload = String.format(
            "transactionId:txn_test|userId:user_test|cardFingerprint:fpr_test" +
            "|amount:%s|currency:USD|merchantId:mch_test" +
            "|merchantCategoryCode:%s|merchantRiskTier:low" +
            "|deviceFingerprint:dev_test|ipAddress:192.168.1.1" +
            "|ipCountry:%s|billingCountry:%s",
            (int) amount, mcc, ipCountry, billingCountry
        );
        return TransactionEvent.from(payload);
    }

    // -------------------------------------------------------------------------
    // Helper: builds an ungrouped Rule using the 4-arg constructor.
    // -------------------------------------------------------------------------
    private Rule ungroupedRule(String name, String expression, int score) {
        return new Rule(name, expression, score, true);
    }

    // -------------------------------------------------------------------------
    // Helper: builds a grouped Rule using the 6-arg constructor.
    // -------------------------------------------------------------------------
    private Rule groupedRule(String name, String expression, int score,
                              String groupName, MatchMode matchMode) {
        return new Rule(name, expression, score, true, groupName, matchMode);
    }

    // =========================================================================
    // UNGROUPED RULE TESTS
    // =========================================================================

    @Test
    void evaluate_ungroupedRule_matches_addsScore() {
        Rule rule = ungroupedRule("high_amount", "amount > 500", 20);
        when(ruleRepository.findByEnabledTrue()).thenReturn(List.of(rule));
        ruleEngine.loadRules();

        TransactionEvent event = buildEvent(600, "5412", "US", "US");
        int score = ruleEngine.evaluate(event);

        assertEquals(20, score, "Ungrouped rule matching amount > 500 should add score 20");
    }

    @Test
    void evaluate_ungroupedRule_doesNotMatch_addsNothing() {
        Rule rule = ungroupedRule("high_amount", "amount > 500", 20);
        when(ruleRepository.findByEnabledTrue()).thenReturn(List.of(rule));
        ruleEngine.loadRules();

        TransactionEvent event = buildEvent(100, "5412", "US", "US");
        int score = ruleEngine.evaluate(event);

        assertEquals(0, score, "Ungrouped rule that does not match should add 0");
    }

    @Test
    void evaluate_multipleUngroupedRules_allMatch_addsAllScores() {
        Rule rule1 = ungroupedRule("high_amount", "amount > 500", 20);
        Rule rule2 = ungroupedRule("country_mismatch", "ipCountry != billingCountry", 15);
        when(ruleRepository.findByEnabledTrue()).thenReturn(List.of(rule1, rule2));
        ruleEngine.loadRules();

        TransactionEvent event = buildEvent(600, "5412", "RU", "US");
        int score = ruleEngine.evaluate(event);

        assertEquals(35, score, "Both ungrouped rules should fire: 20 + 15 = 35");
    }

    @Test
    void evaluate_multipleUngroupedRules_noneMatch_returnsZero() {
        Rule rule1 = ungroupedRule("high_amount", "amount > 500", 20);
        Rule rule2 = ungroupedRule("country_mismatch", "ipCountry != billingCountry", 15);
        when(ruleRepository.findByEnabledTrue()).thenReturn(List.of(rule1, rule2));
        ruleEngine.loadRules();

        TransactionEvent event = buildEvent(100, "5412", "US", "US");
        int score = ruleEngine.evaluate(event);

        assertEquals(0, score, "Neither rule matches — score should be 0");
    }

    @Test
    void evaluate_invalidSpelExpression_doesNotCrash_returnsZero() {
        // A broken SpEL expression must be caught and skipped — not crash the pipeline
        Rule brokenRule = ungroupedRule("broken", "this is not valid spel !!!!", 50);
        when(ruleRepository.findByEnabledTrue()).thenReturn(List.of(brokenRule));
        ruleEngine.loadRules();

        TransactionEvent event = buildEvent(600, "5412", "US", "US");

        assertDoesNotThrow(() -> {
            int score = ruleEngine.evaluate(event);
            assertEquals(0, score, "Bad SpEL expression should be skipped, score = 0");
        });
    }

    @Test
    void evaluate_noRulesLoaded_returnsZero() {
        when(ruleRepository.findByEnabledTrue()).thenReturn(List.of());
        ruleEngine.loadRules();

        TransactionEvent event = buildEvent(600, "5412", "US", "US");
        int score = ruleEngine.evaluate(event);

        assertEquals(0, score, "No rules loaded — score should be 0");
    }

    // =========================================================================
    // ALL GROUP TESTS
    // =========================================================================

    @Test
    void evaluate_allGroup_allMembersMatch_groupFires() {
        Rule signalAmount  = groupedRule("cf_high_amount",     "amount > 500",               0, "coordinated_fraud", MatchMode.ALL);
        Rule signalCountry = groupedRule("cf_country_mismatch","ipCountry != billingCountry", 0, "coordinated_fraud", MatchMode.ALL);
        Rule scoreBearer   = groupedRule("cf_score",           "true",                       60, "coordinated_fraud", MatchMode.ALL);

        when(ruleRepository.findByEnabledTrue()).thenReturn(List.of(signalAmount, signalCountry, scoreBearer));
        ruleEngine.loadRules();

        TransactionEvent event = buildEvent(800, "5412", "RU", "US");
        int score = ruleEngine.evaluate(event);

        assertEquals(60, score, "ALL group: all members match → group fires → score 60");
    }

    @Test
    void evaluate_allGroup_oneMemberFails_groupDoesNotFire() {
        Rule signalAmount  = groupedRule("cf_high_amount",     "amount > 500",               0, "coordinated_fraud", MatchMode.ALL);
        Rule signalCountry = groupedRule("cf_country_mismatch","ipCountry != billingCountry", 0, "coordinated_fraud", MatchMode.ALL);
        Rule scoreBearer   = groupedRule("cf_score",           "true",                       60, "coordinated_fraud", MatchMode.ALL);

        when(ruleRepository.findByEnabledTrue()).thenReturn(List.of(signalAmount, signalCountry, scoreBearer));
        ruleEngine.loadRules();

        // Amount 100 — signalAmount fails, ALL does not fire
        TransactionEvent event = buildEvent(100, "5412", "RU", "US");
        int score = ruleEngine.evaluate(event);

        assertEquals(0, score, "ALL group: one member fails → score 0");
    }

    @Test
    void evaluate_allGroup_noMembersMatch_groupDoesNotFire() {
        Rule signalAmount  = groupedRule("cf_high_amount",     "amount > 500",               0, "coordinated_fraud", MatchMode.ALL);
        Rule signalCountry = groupedRule("cf_country_mismatch","ipCountry != billingCountry", 0, "coordinated_fraud", MatchMode.ALL);
        Rule scoreBearer   = groupedRule("cf_score",           "true",                       60, "coordinated_fraud", MatchMode.ALL);

        when(ruleRepository.findByEnabledTrue()).thenReturn(List.of(signalAmount, signalCountry, scoreBearer));
        ruleEngine.loadRules();

        TransactionEvent event = buildEvent(100, "5412", "US", "US");
        int score = ruleEngine.evaluate(event);

        assertEquals(0, score, "ALL group: no members match → score 0");
    }

    // =========================================================================
    // ANY GROUP TESTS
    // =========================================================================

    @Test
    void evaluate_anyGroup_oneMemberMatches_groupFires() {
        Rule signalGambling = groupedRule("hrc_gambling",   "merchantCategoryCode == '7995'", 0,  "high_risk_category", MatchMode.ANY);
        Rule signalDrug     = groupedRule("hrc_drug_store", "merchantCategoryCode == '5912'", 0,  "high_risk_category", MatchMode.ANY);
        Rule scoreBearer    = groupedRule("hrc_score",      "true",                           25, "high_risk_category", MatchMode.ANY);

        when(ruleRepository.findByEnabledTrue()).thenReturn(List.of(signalGambling, signalDrug, scoreBearer));
        ruleEngine.loadRules();

        TransactionEvent event = buildEvent(100, "7995", "US", "US");
        int score = ruleEngine.evaluate(event);

        assertEquals(25, score, "ANY group: one member matches → group fires → score 25");
    }

    @Test
    void evaluate_anyGroup_noMemberMatches_groupDoesNotFire() {
        Rule signalGambling = groupedRule("hrc_gambling",   "merchantCategoryCode == '7995'", 0,  "high_risk_category", MatchMode.ANY);
        Rule signalDrug     = groupedRule("hrc_drug_store", "merchantCategoryCode == '5912'", 0,  "high_risk_category", MatchMode.ANY);
        Rule scoreBearer    = groupedRule("hrc_score",      "true",                           25, "high_risk_category", MatchMode.ANY);

        when(ruleRepository.findByEnabledTrue()).thenReturn(List.of(signalGambling, signalDrug, scoreBearer));
        ruleEngine.loadRules();

        TransactionEvent event = buildEvent(100, "5412", "US", "US");
        int score = ruleEngine.evaluate(event);

        assertEquals(0, score, "ANY group: no members match → score 0");
    }

    @Test
    void evaluate_anyGroup_allMembersMatch_scoreAddedOnlyOnce() {
        // Both signals match — score must still be added exactly once, not twice
        Rule signalGambling = groupedRule("hrc_gambling",   "merchantCategoryCode == '7995'", 0,  "high_risk_category", MatchMode.ANY);
        Rule signalDrug     = groupedRule("hrc_drug_store", "merchantCategoryCode == '7995'", 0,  "high_risk_category", MatchMode.ANY);
        Rule scoreBearer    = groupedRule("hrc_score",      "true",                           25, "high_risk_category", MatchMode.ANY);

        when(ruleRepository.findByEnabledTrue()).thenReturn(List.of(signalGambling, signalDrug, scoreBearer));
        ruleEngine.loadRules();

        TransactionEvent event = buildEvent(100, "7995", "US", "US");
        int score = ruleEngine.evaluate(event);

        assertEquals(25, score, "ANY group: score added only ONCE even if multiple members match");
    }

    // =========================================================================
    // MIXED: ungrouped + grouped both contribute
    // =========================================================================

    @Test
    void evaluate_ungroupedAndGroupedRules_bothContributeToTotal() {
        Rule ungrouped      = ungroupedRule("high_amount", "amount > 500", 20);
        Rule signalGambling = groupedRule("hrc_gambling",  "merchantCategoryCode == '7995'", 0,  "high_risk_category", MatchMode.ANY);
        Rule scoreBearer    = groupedRule("hrc_score",     "true",                           25, "high_risk_category", MatchMode.ANY);

        when(ruleRepository.findByEnabledTrue()).thenReturn(List.of(ungrouped, signalGambling, scoreBearer));
        ruleEngine.loadRules();

        TransactionEvent event = buildEvent(600, "7995", "US", "US");
        int score = ruleEngine.evaluate(event);

        assertEquals(45, score, "Ungrouped (20) + ANY group (25) = 45");
    }

    // =========================================================================
    // HOT RELOAD TEST
    // =========================================================================

    @Test
    void reload_replacesRulesInMemory() {
        Rule oldRule = ungroupedRule("old_rule", "amount > 1000", 30);
        when(ruleRepository.findByEnabledTrue()).thenReturn(List.of(oldRule));
        ruleEngine.loadRules();

        assertEquals(30, ruleEngine.evaluate(buildEvent(1500, "5412", "US", "US")),
            "Old rule should fire before reload");

        // Simulate DB change
        Rule newRule = ungroupedRule("new_rule", "amount > 2000", 50);
        when(ruleRepository.findByEnabledTrue()).thenReturn(List.of(newRule));
        ruleEngine.reload();

        // Old rule gone
        assertEquals(0, ruleEngine.evaluate(buildEvent(1500, "5412", "US", "US")),
            "After reload, old rule should no longer fire");

        // New rule active
        assertEquals(50, ruleEngine.evaluate(buildEvent(2500, "5412", "US", "US")),
            "After reload, new rule should fire");
    }
}