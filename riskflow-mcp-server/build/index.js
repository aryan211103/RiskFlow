"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const mcp_js_1 = require("@modelcontextprotocol/sdk/server/mcp.js");
const stdio_js_1 = require("@modelcontextprotocol/sdk/server/stdio.js");
const redis_js_1 = require("./redis.js");
const getTransaction_js_1 = require("./tools/getTransaction.js");
const getRiskDecision_js_1 = require("./tools/getRiskDecision.js");
const listPendingReview_js_1 = require("./tools/listPendingReview.js");
const overrideDecision_js_1 = require("./tools/overrideDecision.js");
const getVelocityStats_js_1 = require("./tools/getVelocityStats.js");
const listActiveRules_js_1 = require("./tools/listActiveRules.js");
const reloadRules_js_1 = require("./tools/reloadRules.js");
const getDlqEvents_js_1 = require("./tools/getDlqEvents.js");
const server = new mcp_js_1.McpServer({
    name: "riskflow-mcp-server",
    version: "1.0.0",
});
// Register all 8 tools
(0, getTransaction_js_1.registerGetTransaction)(server);
(0, getRiskDecision_js_1.registerGetRiskDecision)(server);
(0, listPendingReview_js_1.registerListPendingReview)(server);
(0, overrideDecision_js_1.registerOverrideDecision)(server);
(0, getVelocityStats_js_1.registerGetVelocityStats)(server);
(0, listActiveRules_js_1.registerListActiveRules)(server);
(0, reloadRules_js_1.registerReloadRules)(server);
(0, getDlqEvents_js_1.registerGetDlqEvents)(server);
async function main() {
    try {
        await (0, redis_js_1.connectRedis)();
        console.error("Redis connected");
        const transport = new stdio_js_1.StdioServerTransport();
        await server.connect(transport);
        console.error("RiskFlow MCP server running with 8 tools");
    }
    catch (err) {
        console.error("Failed to start MCP server:", err);
        process.exit(1);
    }
}
main();
