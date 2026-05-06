package com.riskflow.scoring.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.riskflow.scoring.service.RuleEngine;

/**
 * Admin REST controller for rule engine management.
 *
 * Exposes a single endpoint:
 *   POST /admin/rules/reload
 *
 * This triggers a hot-reload of all enabled rules from PostgreSQL
 * into the in-memory CopyOnWriteArrayList inside RuleEngine.
 *
 * Why POST and not GET?
 * GET requests should be read-only and idempotent — they should not
 * change server state. A reload changes the in-memory rule set, so
 * POST is the correct HTTP verb.
 *
 * In production this endpoint would be secured behind an admin role.
 * For this project it is open — security is out of scope.
 */
@RestController
@RequestMapping("/admin")
public class RuleAdminController {

    private static final Logger log = LoggerFactory.getLogger(RuleAdminController.class);

    private final RuleEngine ruleEngine;

    public RuleAdminController(RuleEngine ruleEngine) {
        this.ruleEngine = ruleEngine;
    }

    /**
     * Reloads all enabled rules from PostgreSQL into memory.
     *
     * Workflow:
     *   1. Analyst adds or modifies a rule directly in PostgreSQL
     *   2. Analyst calls POST /admin/rules/reload
     *   3. RuleEngine fetches fresh rules and replaces the in-memory list
     *   4. All subsequent transactions are scored with the new rules
     *   5. No service restart required
     *
     * Returns a simple confirmation message with the timestamp.
     */
    @PostMapping("/rules/reload")
    public ResponseEntity<String> reloadRules() {
        log.info("Rule reload requested via admin endpoint");
        ruleEngine.reload();
        return ResponseEntity.ok("Rules reloaded successfully at " + java.time.Instant.now());
    }
}