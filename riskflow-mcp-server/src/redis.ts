import { createClient } from "redis";

// Single Redis client reused across all tool calls
const redisClient = createClient({
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
export async function connectRedis(): Promise<void> {
  await redisClient.connect();
}

export default redisClient;