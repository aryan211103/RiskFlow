"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.connectRedis = connectRedis;
const redis_1 = require("redis");
// Single Redis client reused across all tool calls
const redisClient = (0, redis_1.createClient)({
    socket: {
        host: "localhost",
        port: 6379,
    },
});
redisClient.on("error", (err) => {
    console.error("Redis client error:", err);
});
redisClient.on("connect", () => {
    console.error("Redis connection established");
});
// We export both the client and a connect function
// index.ts will call connect() once at startup before registering tools
async function connectRedis() {
    await redisClient.connect();
}
exports.default = redisClient;
