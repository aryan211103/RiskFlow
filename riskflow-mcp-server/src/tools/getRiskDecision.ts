import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import pool from "../db.js";

export function registerGetRiskDecision(server: McpServer): void {
  server.tool(
    "get_risk_decision",
    "Fetch the risk decision and score breakdown for a transaction. Returns decision type (APPROVED/NEEDS_REVIEW/AUTO_REJECTED), total score, and which rules fired.",
    {
      transaction_id: z.string().describe("The transaction ID to fetch the risk decision for"),
    },
    async ({ transaction_id }) => {
      try {
        const result = await pool.query(
            `SELECT
              transaction_id,
              decision,
              risk_score,
              decision_reason,
              processing_stage,
              decided_at
            FROM risk_decisions
            WHERE transaction_id = $1
            ORDER BY decided_at DESC
            LIMIT 1`,
            [transaction_id]
          );

        if (result.rows.length === 0) {
          return {
            content: [{ type: "text", text: `No risk decision found for transaction: ${transaction_id}` }],
          };
        }

        const d = result.rows[0];

        return {
            content: [{
              type: "text",
              text: [
                `Transaction ID: ${d.transaction_id}`,
                `Decision: ${d.decision}`,
                `Risk Score: ${d.risk_score}`,
                `Reason: ${d.decision_reason}`,
                `Processing Stage: ${d.processing_stage}`,
                `Decided At: ${d.decided_at}`,
              ].join("\n"),
            }],
          };
      } catch (err) {
        console.error("get_risk_decision error:", err);
        return {
          content: [{ type: "text", text: `Error: ${err instanceof Error ? err.message : String(err)}` }],
        };
      }
    }
  );
}