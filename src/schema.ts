// The tables of one storage, in a CockroachDB schema of its own: many storages share one database, one schema each
// (`ego_0`, `ego_1`, ...), so a pod that owns a storage owns a schema. The columns and indexes are those of pi v1.5.2's
// SQLite schema after its two migrations (`sqlite/migrations.ts`), in CockroachDB types:
//
// - INTEGER → INT8. A 0/1 column stays INT8 with its CHECK, so the ported queries bind it unchanged.
// - TEXT holding JSON stays STRING, not JSONB: the storage parses it with `JSON.parse` and needs it back exactly as
//   written (JSONB would reorder keys and turn it into an object on read). `json_valid` checks are dropped.
// - `next_id` stays STRING, as SQLite stores it; it is read with `Number()`.
// - `family` is quoted wherever SQL names it: in CockroachDB a bare FAMILY is a keyword (column families) and a
//   syntax error in SELECT, WHERE and INSERT column lists. The ported queries quote it too.
//
// The schema version is kept in `durable_schema`. A later change of the upstream schema adds a step to `MIGRATIONS`.
import type { Executor } from "./database.ts";

const TABLE_NAMES = {
    durableSchema: "durable_schema",
    durableMetadata: "durable_metadata",
    recordIds: "record_ids",
    conversations: "conversations",
    entries: "entries",
    tasks: "tasks",
    submissions: "submissions",
    documents: "documents",
    documentRevisions: "document_revisions",
    leases: "leases",
} as const;

type Tables = { readonly [K in keyof typeof TABLE_NAMES]: string };

/** The qualified table names of one storage's schema, as the ported queries use them. */
export type CockroachSchema = { readonly name: string; readonly tables: Tables };

const SCHEMA_NAME = /^[a-z_][a-z0-9_]{0,62}$/;

export function createCockroachSchema(name: string): CockroachSchema {
    if (!SCHEMA_NAME.test(name) || name.startsWith("pg_") || name === "public" || name === "crdb_internal") {
        throw new Error(`Invalid storage schema name: ${name} (lowercase letters, digits and _, not reserved)`);
    }
    const tables = Object.fromEntries(
        Object.entries(TABLE_NAMES).map(([key, table]) => [key, `${name}.${table}`]),
    ) as Tables;
    return { name, tables };
}

/** The DDL of version `version` of the schema, in order. */
const MIGRATIONS = (schema: CockroachSchema): ReadonlyArray<{ version: number; statements: readonly string[] }> => {
    const t = schema.tables;
    return [
        {
            version: 2,
            statements: [
                `CREATE TABLE IF NOT EXISTS ${t.durableMetadata} (
                    singleton INT8 PRIMARY KEY CHECK (singleton = 1),
                    next_id STRING NOT NULL,
                    next_seq INT8 NOT NULL
                )`,
                `INSERT INTO ${t.durableMetadata} (singleton, next_id, next_seq) VALUES (1, '2', 1) ON CONFLICT (singleton) DO NOTHING`,
                `CREATE TABLE IF NOT EXISTS ${t.recordIds} (
                    id INT8 PRIMARY KEY,
                    record_type STRING NOT NULL CHECK (record_type IN ('conversation', 'entry', 'task', 'submission', 'document'))
                )`,
                `CREATE TABLE IF NOT EXISTS ${t.conversations} (
                    id INT8 PRIMARY KEY,
                    owner_conversation_id INT8,
                    owner_task_id INT8,
                    record STRING NOT NULL,
                    INDEX conversations_by_owner_conversation (owner_conversation_id, id),
                    INDEX conversations_by_owner_task (owner_task_id, id)
                )`,
                `CREATE TABLE IF NOT EXISTS ${t.entries} (
                    id INT8 PRIMARY KEY,
                    conversation_id INT8 NOT NULL,
                    head INT8,
                    commit_seq INT8 NOT NULL,
                    record STRING NOT NULL,
                    INDEX entries_by_conversation (conversation_id, id DESC),
                    INDEX entry_heads_by_conversation (conversation_id, id DESC) WHERE head IS NOT NULL
                )`,
                `CREATE TABLE IF NOT EXISTS ${t.tasks} (
                    id INT8 PRIMARY KEY,
                    conversation_id INT8 NOT NULL,
                    kind STRING NOT NULL,
                    status STRING NOT NULL CHECK (status IN ('pending', 'running', 'waiting', 'sleeping', 'completing', 'terminal')),
                    abort_requested INT8 NOT NULL CHECK (abort_requested IN (0, 1)),
                    background INT8 NOT NULL CHECK (background IN (0, 1)),
                    record STRING NOT NULL,
                    INDEX tasks_by_status (status, id),
                    INDEX tasks_by_conversation (conversation_id, id),
                    INDEX tasks_by_kind (kind, id),
                    INDEX tasks_by_abort_requested (abort_requested, id),
                    INDEX tasks_by_background (background, id)
                )`,
                `CREATE TABLE IF NOT EXISTS ${t.submissions} (
                    id INT8 PRIMARY KEY,
                    conversation_id INT8 NOT NULL,
                    request_id STRING,
                    status STRING NOT NULL CHECK (status IN ('queued', 'placed', 'done', 'unanswered')),
                    record STRING NOT NULL,
                    INDEX submissions_by_request (conversation_id, request_id),
                    INDEX submissions_by_conversation (conversation_id, id),
                    INDEX submissions_by_status (status, id)
                )`,
                `CREATE TABLE IF NOT EXISTS ${t.documents} (
                    id INT8 PRIMARY KEY,
                    kind STRING NOT NULL,
                    "family" INT8 NOT NULL CHECK ("family" IN (0, 1)),
                    key_value STRING NOT NULL,
                    scope_kind STRING NOT NULL CHECK (scope_kind IN ('session', 'conversation', 'task')),
                    owner_id INT8 NOT NULL,
                    created_at INT8 NOT NULL,
                    retired_at INT8,
                    record STRING NOT NULL,
                    INDEX documents_by_address (kind, scope_kind, owner_id, "family", key_value, created_at DESC, retired_at),
                    INDEX documents_by_scope (scope_kind, owner_id, id),
                    INDEX documents_by_scope_kind (scope_kind, owner_id, kind, id)
                )`,
                `CREATE TABLE IF NOT EXISTS ${t.documentRevisions} (
                    document_id INT8 NOT NULL,
                    seq INT8 NOT NULL,
                    kind STRING NOT NULL CHECK (kind IN ('base', 'delta')),
                    version INT8 NOT NULL,
                    content STRING NOT NULL,
                    PRIMARY KEY (document_id, seq),
                    INDEX document_revisions_by_kind (document_id, kind, seq DESC)
                )`,
                // Not upstream: the lease of the storage (`lease.ts`), one row.
                `CREATE TABLE IF NOT EXISTS ${t.leases} (
                    singleton INT8 PRIMARY KEY CHECK (singleton = 1),
                    owner STRING NOT NULL,
                    epoch INT8 NOT NULL,
                    expires_at TIMESTAMPTZ NOT NULL
                )`,
            ],
        },
    ];
};

export const CURRENT_SCHEMA_VERSION = 2;

/**
 * Creates the schema and its tables, or brings them to the current version. Every statement is idempotent, so two
 * processes that open the same storage at once both succeed (the lease then lets one of them in). DDL runs outside a
 * transaction: CockroachDB runs schema changes best one statement at a time.
 */
export async function migrate(executor: Executor, schema: CockroachSchema): Promise<void> {
    await executor.exec(`CREATE SCHEMA IF NOT EXISTS ${schema.name}`);
    await executor.exec(`CREATE TABLE IF NOT EXISTS ${schema.tables.durableSchema} (version INT8 PRIMARY KEY)`);
    const row = await executor.get<{ version: number | null }>(
        `SELECT max(version) AS version FROM ${schema.tables.durableSchema}`,
    );
    const current = row?.version ?? 0;
    if (current > CURRENT_SCHEMA_VERSION) {
        throw new Error(`Storage schema ${schema.name} is at version ${current}, newer than this code (${CURRENT_SCHEMA_VERSION})`);
    }
    for (const migration of MIGRATIONS(schema)) {
        if (migration.version <= current) continue;
        for (const statement of migration.statements) await executor.exec(statement);
        await executor.run(
            `INSERT INTO ${schema.tables.durableSchema} (version) VALUES (?) ON CONFLICT (version) DO NOTHING`,
            migration.version,
        );
    }
}
