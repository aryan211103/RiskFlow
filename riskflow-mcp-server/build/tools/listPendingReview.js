"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.registerListPendingReview = registerListPendingReview;
const zod_1 = require("zod");
const db_js_1 = __importDefault(require("../db.js"));
function registerListPendingReview(server) {
    server.tool("list_pending_review", "List all transactions with a NEEDS_REVIEW decision that are awaiting analyst action. Returns transaction ID, score, and timestamp.", {
        limit: zod_1.z.number().optional().describe("Maximum number of results to return. Defaults to 20."),
    }, async ({ limit }) => {
        try {
            const maxRows = limit ?? 20;
            const result = await db_js_1.default.query(`SELECT
            rd.transaction_id,
            rd.total_score,
            rd.created_at,
            t.amount,
            t.currency,
            t.merchant_id,
            t.ip_country,
            t.billing_country
          FROM risk_decisions rd
          JOIN transactions t ON t.id = rd.transaction_id
          WHERE rd.decision = 'NEEDS_REVIEW'
          ORDER BY rd.created_at DESC
          LIMIT $1`, [maxRows]);
            if (result.rows.length === 0) {
                return {
                    content: [{ type: "text", text: "No transactions currently pending review." }],
                };
            }
            const lines = result.rows.map((r, i) => [
                `${i + 1}. Transaction: ${r.transaction_id}`,
                `   Score: ${r.total_score} | Amount: ${r.amount} ${r.currency}`,
                `   Merchant: ${r.merchant_id} | IP Country: ${r.ip_country} | Billing Country: ${r.billing_country}`,
                `   Flagged At: ${r.created_at}`,
            ].join("\n"));
            return {
                content: [
                    {
                        type: "text",
                        text: `${result.rows.length} transaction(s) pending review:\n\n${lines.join("\n\n")}`,
                    },
                ],
            };
        }
        catch (err) {
            console.error("list_pending_review error:", err);
            return {
                content: [{ type: "text", text: `Error: ${err instanceof Error ? err.message : String(err)}` }],
            };
        }
    });
}
