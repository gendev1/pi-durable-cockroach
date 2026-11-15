// A fresh storage schema per test in the local CockroachDB (`npm run db:up`), dropped afterwards.
import { randomBytes } from "node:crypto";
import pg from "pg";
import { CockroachDatabase } from "../src/database.ts";

export const URL = process.env.COCKROACH_URL ?? "postgresql://root@localhost:26257/defaultdb?sslmode=disable";

export const freshSchema = () => `t_${randomBytes(6).toString("hex")}`;

export const openDatabase = () => CockroachDatabase.open({ connection: URL });

export async function dropSchema(name: string): Promise<void> {
    const client = new pg.Client({ connectionString: URL });
    await client.connect();
    try {
        await client.query(`DROP SCHEMA IF EXISTS ${name} CASCADE`);
    } finally {
        await client.end();
    }
}
