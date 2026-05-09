import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { connectRedis } from "./redis.js";

import { registerGetTransaction } from "./tools/getTransaction.js";
import { registerGetRiskDecision } from "./tools/getRiskDecision.js";
import { registerListPendingReview } from "./tools/listPendingReview.js";
import { registerOverrideDecision } from "./tools/overrideDecision.js";
import { registerGetVelocityStats } from "./tools/getVelocityStats.js";
import { registerListActiveRules } from "./tools/listActiveRules.js";
import { registerReloadRules } from "./tools/reloadRules.js";
import { registerGetDlqEvents } from "./tools/getDlqEvents.js";

const server = new McpServer({
  name: "riskflow-mcp-server",
  version: "1.0.0",
});

// Register all 8 tools
registerGetTransaction(server);
registerGetRiskDecision(server);
registerListPendingReview(server);
registerOverrideDecision(server);
registerGetVelocityStats(server);
registerListActiveRules(server);
registerReloadRules(server);
registerGetDlqEvents(server);

async function main() {
  try {
    await connectRedis();
    console.error("Redis connected");

    const transport = new StdioServerTransport();
    await server.connect(transport);

    console.error("RiskFlow MCP server running with 8 tools");
  } catch (err) {
    console.error("Failed to start MCP server:", err);
    process.exit(1);
  }
}

main();