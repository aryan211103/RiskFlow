import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import pool from "../db.js";

export function registerListPendingReview(server: McpServer): void {
  server.tool(
    "list_pending_review",
    "List all transactions with a NEEDS_REVIEW decision that are awaiting analyst action. Returns transaction ID, score, and timestamp.",
    {
      limit: z.number().optional().describe("Maximum number of results to return. Defaults to 20."),
    },
    async ({ limit }) => {
      try {
        const maxRows = limit ?? 20;

        const result = await pool.query(
          `SELECT
            rd.transaction_id,
            rd.risk_score,
            rd.decided_at,
            t.amount,
            t.currency,
            t.merchant_id,
            t.ip_country,
            t.billing_country
          FROM risk_decisions rd
          JOIN transactions t ON t.transaction_id = rd.transaction_id
          WHERE rd.decision = 'NEEDS_REVIEW'
          ORDER BY rd.decided_at DESC
          LIMIT $1`,
          [maxRows]
        );

        if (result.rows.length === 0) {
          return {
            content: [{ type: "text", text: "No transactions currently pending review." }],
          };
        }

        const lines = result.rows.map((r, i) =>
            [
              `${i + 1}. Transaction: ${r.transaction_id}`,
              `   Score: ${r.risk_score} | Amount: ${r.amount} ${r.currency}`,
              `   Merchant: ${r.merchant_id} | IP Country: ${r.ip_country} | Billing Country: ${r.billing_country}`,
              `   Flagged At: ${r.decided_at}`,
            ].join("\n")
          );

        return {
          content: [
            {
              type: "text",
              text: `${result.rows.length} transaction(s) pending review:\n\n${lines.join("\n\n")}`,
            },
          ],
        };
      } catch (err) {
        console.error("list_pending_review error:", err);
        return {
          content: [{ type: "text", text: `Error: ${err instanceof Error ? err.message : String(err)}` }],
        };
      }
    }
  );
}