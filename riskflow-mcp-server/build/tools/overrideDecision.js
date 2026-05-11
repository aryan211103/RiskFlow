"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.registerOverrideDecision = registerOverrideDecision;
const zod_1 = require("zod");
const db_js_1 = __importDefault(require("../db.js"));
function registerOverrideDecision(server) {
    server.tool("override_decision", "Override an AUTO_REJECTED or NEEDS_REVIEW decision with a manual analyst decision. Requires a reason. This creates an audit trail in the database.", {
        transaction_id: zod_1.z.string().describe("The transaction ID to override"),
        new_decision: zod_1.z.enum(["APPROVED", "AUTO_REJECTED"]).describe("The new decision to apply"),
        reason: zod_1.z.string().describe("Reason for the override, e.g. 'Verified with customer, legitimate purchase'"),
        analyst_id: zod_1.z.string().describe("ID or name of the analyst making the override"),
    }, async ({ transaction_id, new_decision, reason, analyst_id }) => {
        try {
            // First check the decision exists
            const check = await db_js_1.default.query(`SELECT id, decision FROM risk_decisions WHERE transaction_id = $1 ORDER BY decided_at DESC LIMIT 1`, [transaction_id]);
            if (check.rows.length === 0) {
                return {
                    content: [{ type: "text", text: `No risk decision found for transaction: ${transaction_id}` }],
                };
            }
            const current = check.rows[0];
            // Only allow overriding NEEDS_REVIEW or AUTO_REJECTED
            if (current.decision === "APPROVED" && new_decision === "APPROVED") {
                return {
                    content: [{ type: "text", text: `Transaction ${transaction_id} is already APPROVED. No override needed.` }],
                };
            }
            // Update the decision and write audit fields
            await db_js_1.default.query(`UPDATE risk_decisions
          SET
            decision = $1,
            override_reason = $2,
            override_by = $3,
            overridden_at = NOW()
          WHERE transaction_id = $4`, [new_decision, reason, analyst_id, transaction_id]);
            return {
                content: [
                    {
                        type: "text",
                        text: [
                            `Override applied successfully.`,
                            `Transaction ID: ${transaction_id}`,
                            `Previous Decision: ${current.decision}`,
                            `New Decision: ${new_decision}`,
                            `Reason: ${reason}`,
                            `Analyst: ${analyst_id}`,
                            `Overridden At: ${new Date().toISOString()}`,
                        ].join("\n"),
                    },
                ],
            };
        }
        catch (err) {
            console.error("override_decision error:", err);
            return {
                content: [{ type: "text", text: `Error: ${err instanceof Error ? err.message : String(err)}` }],
            };
        }
    });
}
