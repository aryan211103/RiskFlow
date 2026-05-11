import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import pool from "../db.js";

// This function attaches the get_transaction tool to the MCP server
// Claude Desktop will call this tool when an analyst asks for a transaction by ID
export function registerGetTransaction(server: McpServer): void {
  server.tool(
    // Tool name — this is what Claude sees and calls
    "get_transaction",

    // Tool description — Claude reads this to decide when to use this tool
    // Write it clearly, Claude uses it to match analyst intent to the right tool
    "Fetch a transaction by its ID. Returns transaction details including amount, merchant, card fingerprint, IP address, and current status.",

    // Input schema — defines what parameters this tool accepts
    // z.object() is Zod, a TypeScript validation library bundled with the MCP SDK
    // Claude will fill these fields based on what the analyst says
    {
      transaction_id: z.string().describe("The transaction ID to look up, e.g. txn_abc123"),
    },

    // Handler — the actual function that runs when Claude calls this tool
    // input is typed based on the schema above
    async ({ transaction_id }) => {
      try {
        // Query the transactions table in PostgreSQL
        // $1 is a parameterized query — prevents SQL injection
        const result = await pool.query(
          `SELECT 
            id,
            user_id,
            card_fingerprint,
            amount,
            currency,
            merchant_id,
            merchant_category_code,
            merchant_risk_tier,
            device_fingerprint,
            ip_address,
            ip_country,
            billing_country,
            status,
            created_at
          FROM transactions
          WHERE transaction_id = $1`,
          [transaction_id]
        );

        // No rows means the transaction ID doesn't exist
        if (result.rows.length === 0) {
          return {
            content: [
              {
                type: "text",
                text: `No transaction found with ID: ${transaction_id}`,
              },
            ],
          };
        }

        const tx = result.rows[0];

        // Format the result as readable text for the analyst
        // Claude will present this to the user
        return {
          content: [
            {
              type: "text",
              text: [
                `Transaction ID: ${tx.id}`,
                `Status: ${tx.status}`,
                `Amount: ${tx.amount} ${tx.currency}`,
                `Merchant ID: ${tx.merchant_id}`,
                `Merchant Category Code: ${tx.merchant_category_code}`,
                `Merchant Risk Tier: ${tx.merchant_risk_tier}`,
                `Card Fingerprint: ${tx.card_fingerprint}`,
                `Device Fingerprint: ${tx.device_fingerprint}`,
                `IP Address: ${tx.ip_address}`,
                `IP Country: ${tx.ip_country}`,
                `Billing Country: ${tx.billing_country}`,
                `Created At: ${tx.created_at}`,
              ].join("\n"),
            },
          ],
        };
      } catch (err) {
        // Always catch and return errors as text
        // Throwing an unhandled error crashes the MCP server process
        console.error("get_transaction error:", err);
        return {
          content: [
            {
              type: "text",
              text: `Error fetching transaction: ${err instanceof Error ? err.message : String(err)}`,
            },
          ],
        };
      }
    }
  );
}