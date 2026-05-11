"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.registerListActiveRules = registerListActiveRules;
const zod_1 = require("zod");
const db_js_1 = __importDefault(require("../db.js"));
function registerListActiveRules(server) {
    server.tool("list_active_rules", "List all enabled rules from PostgreSQL with their SpEL expressions, scores, and group assignments.", {
        include_disabled: zod_1.z.boolean().optional().describe("Set to true to also show disabled rules. Defaults to false."),
    }, async ({ include_disabled }) => {
        try {
            const query = include_disabled
                ? `SELECT id, name, expression, score, enabled, group_name, match_mode 
             FROM rules ORDER BY group_name NULLS FIRST, name`
                : `SELECT id, name, expression, score, enabled, group_name, match_mode 
             FROM rules WHERE enabled = true ORDER BY group_name NULLS FIRST, name`;
            const result = await db_js_1.default.query(query);
            if (result.rows.length === 0) {
                return {
                    content: [{ type: "text", text: "No rules found." }],
                };
            }
            const lines = result.rows.map((r) => [
                `Rule: ${r.name} | Score: ${r.score} | Enabled: ${r.enabled}`,
                `Expression: ${r.expression}`,
                r.group_name ? `Group: ${r.group_name} (${r.match_mode})` : `Group: none`,
            ].join("\n"));
            return {
                content: [
                    {
                        type: "text",
                        text: `${result.rows.length} rule(s) found:\n\n${lines.join("\n\n")}`,
                    },
                ],
            };
        }
        catch (err) {
            console.error("list_active_rules error:", err);
            return {
                content: [{ type: "text", text: `Error: ${err instanceof Error ? err.message : String(err)}` }],
            };
        }
    });
}
