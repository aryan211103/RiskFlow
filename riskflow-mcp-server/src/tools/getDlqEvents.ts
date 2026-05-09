import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import pool from "../db.js";

export function registerGetDlqEvents(server: McpServer): void {
  server.tool(
    "get_dlq_events",
    "List recent failed events from the DLQ processor. Shows failure type, classification, retry count, and current status.",
    {
      limit: z.number().optional().describe("Maximum number of results to return. Defaults to 20."),
      status: z.enum(["FAILED", "RETRYING", "QUARANTINED", "RESOLVED"]).optional().describe("Filter by event status"),
    },
    async ({ limit, status }) => {
      try {
        const maxRows = limit ?? 20;

        const result = status
          ? await pool.query(
              `SELECT
                id,
                transaction_id,
                failure_type,
                status,
                retry_count,
                error_message,
                created_at
              FROM failed_events
              WHERE status = $1
              ORDER BY created_at DESC
              LIMIT $2`,
              [status, maxRows]
            )
          : await pool.query(
              `SELECT
                id,
                transaction_id,
                failure_type,
                status,
                retry_count,
                error_message,
                created_at
              FROM failed_events
              ORDER BY created_at DESC
              LIMIT $1`,
              [maxRows]
            );

        if (result.rows.length === 0) {
          return {
            content: [{ type: "text", text: "No DLQ events found." }],
          };
        }

        const lines = result.rows.map((r, i) =>
          [
            `${i + 1}. Transaction: ${r.transaction_id}`,
            `   Failure Type: ${r.failure_type} | Status: ${r.status} | Retries: ${r.retry_count}`,
            `   Error: ${r.error_message ?? "none"}`,
            `   Created At: ${r.created_at}`,
          ].join("\n")
        );

        return {
          content: [
            {
              type: "text",
              text: `${result.rows.length} DLQ event(s):\n\n${lines.join("\n\n")}`,
            },
          ],
        };
      } catch (err) {
        console.error("get_dlq_events error:", err);
        return {
          content: [{ type: "text", text: `Error: ${err instanceof Error ? err.message : String(err)}` }],
        };
      }
    }
  );
}