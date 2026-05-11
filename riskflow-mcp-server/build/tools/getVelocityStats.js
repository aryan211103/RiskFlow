"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.registerGetVelocityStats = registerGetVelocityStats;
const zod_1 = require("zod");
const redis_js_1 = __importDefault(require("../redis.js"));
function registerGetVelocityStats(server) {
    server.tool("get_velocity_stats", "Fetch Redis velocity counters for a given card fingerprint. Shows how many transactions the card has made in the last 1 minute, 5 minutes, and 1 hour.", {
        card_fingerprint: zod_1.z.string().describe("The card fingerprint to look up velocity stats for, e.g. fpr_abc123"),
    }, async ({ card_fingerprint }) => {
        try {
            const now = Date.now();
            // These key patterns must match exactly what BehavioralAnalyzer.java writes
            // The sorted set stores transaction timestamps as scores
            // We count members within a time window using ZCOUNT
            const oneMinuteAgo = now - 60 * 1000;
            const fiveMinutesAgo = now - 5 * 60 * 1000;
            const oneHourAgo = now - 60 * 60 * 1000;
            const key = `velocity:card:${card_fingerprint}`;
            // ZCOUNT key min max — counts members with score between min and max
            // Scores in this sorted set are Unix timestamps in milliseconds
            const [count1m, count5m, count1h, total] = await Promise.all([
                redis_js_1.default.zCount(key, oneMinuteAgo, now),
                redis_js_1.default.zCount(key, fiveMinutesAgo, now),
                redis_js_1.default.zCount(key, oneHourAgo, now),
                redis_js_1.default.zCard(key), // total entries in the set
            ]);
            return {
                content: [
                    {
                        type: "text",
                        text: [
                            `Velocity Stats for Card: ${card_fingerprint}`,
                            `Transactions in last 1 minute:  ${count1m}`,
                            `Transactions in last 5 minutes: ${count5m}`,
                            `Transactions in last 1 hour:    ${count1h}`,
                            `Total entries in Redis:         ${total}`,
                        ].join("\n"),
                    },
                ],
            };
        }
        catch (err) {
            console.error("get_velocity_stats error:", err);
            return {
                content: [{ type: "text", text: `Error: ${err instanceof Error ? err.message : String(err)}` }],
            };
        }
    });
}
