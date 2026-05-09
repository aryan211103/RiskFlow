"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const pg_1 = require("pg");
// Pool maintains a set of reusable PostgreSQL connections
// Instead of opening a new connection for every query (slow),
// we open a few once and reuse them across all tool calls
const pool = new pg_1.Pool({
    host: "localhost",
    port: 5432,
    database: "riskflow",
    user: "postgres",
    password: "secret",
    max: 10, // maximum 10 concurrent connections
    idleTimeoutMillis: 30000, // close idle connections after 30s
    connectionTimeoutMillis: 2000, // fail fast if DB is unreachable
});
// Test the connection when the server starts
pool.on("connect", () => {
    console.error("PostgreSQL connection established");
});
pool.on("error", (err) => {
    console.error("PostgreSQL pool error:", err);
});
exports.default = pool;
