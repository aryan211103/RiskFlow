import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import redisClient from "../redis.js";

export function registerGetVelocityStats(server: McpServer): void {
  server.tool(
    "get_velocity_stats",
    "Fetch Redis velocity counters for a given card fingerprint. Shows how many transactions the card has made in the last 1 minute, 5 minutes, and 1 hour.",
    {
      card_fingerprint: z.string().describe("The card fingerprint to look up velocity stats for, e.g. fpr_abc123"),
    },
    async ({ card_fingerprint }) => {
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
          redisClient.zCount(key, oneMinuteAgo, now),
          redisClient.zCount(key, fiveMinutesAgo, now),
          redisClient.zCount(key, oneHourAgo, now),
          redisClient.zCard(key), // total entries in the set
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
      } catch (err) {
        console.error("get_velocity_stats error:", err);
        return {
          content: [{ type: "text", text: `Error: ${err instanceof Error ? err.message : String(err)}` }],
        };
      }
    }
  );
}