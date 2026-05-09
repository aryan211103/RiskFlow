"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.registerReloadRules = registerReloadRules;
const zod_1 = require("zod");
function registerReloadRules(server) {
    server.tool("reload_rules", "Trigger a live rule reload on the risk scoring service. Rules are reloaded from PostgreSQL without restarting the service. Use this after updating rules in the database.", {
        confirm: zod_1.z.boolean().describe("Must be set to true to confirm the reload action"),
    }, async ({ confirm }) => {
        try {
            if (!confirm) {
                return {
                    content: [{ type: "text", text: "Reload cancelled. Set confirm to true to proceed." }],
                };
            }
            // This is the one tool that calls the REST API instead of the DB directly
            // The reload logic lives inside the scoring service, not in SQL
            const response = await fetch("http://localhost:8083/admin/rules/reload", {
                method: "POST",
            });
            if (!response.ok) {
                return {
                    content: [{ type: "text", text: `Reload failed. Scoring service returned HTTP ${response.status}` }],
                };
            }
            const body = await response.text();
            return {
                content: [
                    {
                        type: "text",
                        text: [`Rule reload triggered successfully.`, `Scoring service response: ${body}`].join("\n"),
                    },
                ],
            };
        }
        catch (err) {
            console.error("reload_rules error:", err);
            return {
                content: [
                    {
                        type: "text",
                        text: `Error: Could not reach scoring service. Is it running on port 8083? ${err instanceof Error ? err.message : String(err)}`,
                    },
                ],
            };
        }
    });
}
