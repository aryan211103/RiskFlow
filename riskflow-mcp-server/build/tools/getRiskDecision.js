"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.registerGetRiskDecision = registerGetRiskDecision;
const zod_1 = require("zod");
const db_js_1 = __importDefault(require("../db.js"));
function registerGetRiskDecision(server) {
    server.tool("get_risk_decision", "Fetch the risk decision and score breakdown for a transaction. Returns decision type (APPROVED/NEEDS_REVIEW/AUTO_REJECTED), total score, and which rules fired.", {
        transaction_id: zod_1.z.string().describe("The transaction ID to fetch the risk decision for"),
    }, async ({ transaction_id }) => {
        try {
            const result = await db_js_1.default.query(`SELECT
            id,
            transaction_id,
            decision,
            total_score,
            hard_rule_triggered,
            behavioral_score,
            rule_engine_score,
            enrichment_score,
            created_at
          FROM risk_decisions
          WHERE transaction_id = $1
          ORDER BY created_at DESC
          LIMIT 1`, [transaction_id]);
            if (result.rows.length === 0) {
                return {
                    content: [{ type: "text", text: `No risk decision found for transaction: ${transaction_id}` }],
                };
            }
            const d = result.rows[0];
            return {
                content: [
                    {
                        type: "text",
                        text: [
                            `Transaction ID: ${d.transaction_id}`,
                            `Decision: ${d.decision}`,
                            `Total Score: ${d.total_score}`,
                            `Hard Rule Triggered: ${d.hard_rule_triggered ?? "none"}`,
                            `Behavioral Score: ${d.behavioral_score}`,
                            `Rule Engine Score: ${d.rule_engine_score}`,
                            `Enrichment Score: ${d.enrichment_score}`,
                            `Decided At: ${d.created_at}`,
                        ].join("\n"),
                    },
                ],
            };
        }
        catch (err) {
            console.error("get_risk_decision error:", err);
            return {
                content: [{ type: "text", text: `Error: ${err instanceof Error ? err.message : String(err)}` }],
            };
        }
    });
}
