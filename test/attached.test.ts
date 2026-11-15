// Attached writes: the application's own rows are stored in the transaction of the pi-durable commit they belong
// to, or not at all. Run against the local CockroachDB (`npm run db:up`).
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { createModels, fauxProvider } from "@earendil-works/pi-ai";
import { createRegistry, defineDoc, Harness } from "@earendil-works/pi-durable";
import pg from "pg";
import { afterEach, expect, test } from "vitest";
import { AttachedWrites, AttachedWritesFailed, AttachedWritesMissing } from "../src/attached.ts";
import { LeaseHeld } from "../src/lease.ts";
import { openCockroachStorage } from "../src/open.ts";
import { dropSchema, freshSchema, URL } from "./helpers.ts";

const context = BACKGROUND_CONTEXT;
const cleanup: Array<() => Promise<unknown>> = [];

afterEach(async () => {
    for (const step of cleanup.splice(0).reverse()) await step().catch(() => undefined);
});

/** Something pi-durable stores in the same commit: the checkpoint of the application's step, in these tests. */
const Step = defineDoc<{ done: number }>({
    kind: "test.step",
    version: 1,
    scope: "session",
    initial: () => ({ done: 0 }),
    checkpointWhen: () => false,
});

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** One query on a connection of its own: what a reader outside the storage sees. */
const query = async <T extends object>(sql: string): Promise<T[]> => {
    const client = new pg.Client({ connectionString: URL });
    await client.connect();
    try {
        return (await client.query(sql)).rows as T[];
    } finally {
        await client.end();
    }
};

/** A storage schema, and beside it the application's own schema with one table. */
const schemasForTest = async () => {
    const schema = freshSchema();
    const app = `${schema}_app`;
    cleanup.push(() => dropSchema(schema), () => dropSchema(app));
    await query(`CREATE SCHEMA ${app}`);
    await query(`CREATE TABLE ${app}.note (id STRING PRIMARY KEY, body STRING NOT NULL)`);
    return { schema, app };
};

const open = async (schema: string, owner = "pod-a", options: { now?: () => number; ttlMs?: number } = {}) => {
    // A short lease: a test that fails a Session reopens the storage without waiting long if the release failed.
    const opened = await openCockroachStorage({ connection: URL, schema, owner, ttlMs: 2_000, attached: options });
    cleanup.push(() => opened.storage.close(context));
    const models = createModels();
    models.setProvider(fauxProvider().provider);
    const harness = await Harness.open(opened.storage, { models, registry: createRegistry() }, context);
    cleanup.push(() => harness.close(context));
    const root = await harness.root(context);
    return { ...opened, harness, root };
};

/** Open again after the first owner is gone, waiting out its lease if it could not release it. */
const reopen = async (schema: string) => {
    for (let attempt = 0; ; attempt++) {
        try {
            return await open(schema, "pod-b");
        } catch (error) {
            if (!(error instanceof LeaseHeld) || attempt > 5) throw error;
            await sleep(1_000);
        }
    }
};

const notes = (app: string) => query<{ id: string; body: string }>(`SELECT id, body FROM ${app}.note ORDER BY id`);

test("a row attached to a commit is stored with what the commit stores", async () => {
    const { schema, app } = await schemasForTest();
    const { root, harness, attached } = await open(schema);

    await root.commit(async (tx) => {
        await attached.attach(tx, (sql) => sql.run(`INSERT INTO ${app}.note (id, body) VALUES (?, ?)`, "n1", "first"));
        (await tx.doc(Step)).done = 1;
    }, context);

    expect(await notes(app)).toEqual([{ id: "n1", body: "first" }]);
    expect((await harness.snapshot(Step, context))?.done).toBe(1);
    expect(attached.waiting).toBe(0);
});

test("when an attached statement fails, nothing of the commit is stored", async () => {
    const { schema, app } = await schemasForTest();
    const first = await open(schema);
    await first.root.commit(async (tx) => {
        (await tx.doc(Step)).done = 1;
    }, context);

    const failing = first.root.commit(async (tx) => {
        await first.attached.attach(tx, async (sql) => {
            await sql.run(`INSERT INTO ${app}.note (id, body) VALUES (?, ?)`, "n1", "kept only with its commit");
            await sql.run(`INSERT INTO ${app}.missing_table (id) VALUES (?)`, "x");
        });
        (await tx.doc(Step)).done = 2;
    }, context);
    await expect(failing).rejects.toBeInstanceOf(AttachedWritesFailed);
    expect(first.attached.waiting).toBe(0);

    // pi-durable fails a Session whose commit was rejected; the application reopens it.
    await first.harness.close(context).catch(() => undefined);
    await first.storage.close(context).catch(() => undefined);
    const second = await reopen(schema);

    expect(await notes(app)).toEqual([]);
    expect((await second.harness.snapshot(Step, context))?.done).toBe(1);
});

test("a commit CockroachDB runs again runs its attached write again and stores it once", async () => {
    const { schema, app } = await schemasForTest();
    const { root, harness, attached, database } = await open(schema);
    // For the test only: `crdb_internal.force_retry` below needs it in this CockroachDB version.
    await database.exec("SET allow_unsafe_internals = true");
    let runs = 0;

    await root.commit(async (tx) => {
        await attached.attach(tx, async (sql) => {
            runs++;
            await sql.run(`INSERT INTO ${app}.note (id, body) VALUES (?, ?)`, "n1", "once");
            // After the row is written, the first attempt is failed with a serialization error (40001), as
            // CockroachDB does to a transaction it has to restart. The attempt's row is rolled back with it.
            if (runs === 1) await sql.get("SELECT crdb_internal.force_retry('1h')");
        });
        (await tx.doc(Step)).done = 1;
    }, context);

    expect(database.retries).toBe(1);
    expect(runs).toBe(2);
    expect(await notes(app)).toEqual([{ id: "n1", body: "once" }]);
    expect((await harness.snapshot(Step, context))?.done).toBe(1);
});

test("several writes attached to one commit run in order, and the next commit's run with it", async () => {
    const { schema, app } = await schemasForTest();
    const { root, attached } = await open(schema);

    await root.commit(async (tx) => {
        await attached.attach(tx, (sql) => sql.run(`INSERT INTO ${app}.note (id, body) VALUES (?, ?)`, "n1", "a"));
        await attached.attach(tx, (sql) => sql.run(`UPDATE ${app}.note SET body = ? WHERE id = ?`, "a, then b", "n1"));
    }, context);
    await root.commit(async (tx) => {
        await attached.attach(tx, (sql) => sql.run(`INSERT INTO ${app}.note (id, body) VALUES (?, ?)`, "n2", "c"));
    }, context);

    expect(await notes(app)).toEqual([
        { id: "n1", body: "a, then b" },
        { id: "n2", body: "c" },
    ]);
    expect(attached.waiting).toBe(0);
});

test("commits made at the same time each store their own attached write", async () => {
    const { schema, app } = await schemasForTest();
    const { root, harness, attached } = await open(schema);

    await Promise.all(
        ["n1", "n2", "n3", "n4"].map((id) =>
            root.commit(async (tx) => {
                await attached.attach(tx, (sql) => sql.run(`INSERT INTO ${app}.note (id, body) VALUES (?, ?)`, id, id));
                (await tx.doc(Step)).done += 1;
            }, context),
        ),
    );

    expect((await notes(app)).map((note) => note.id)).toEqual(["n1", "n2", "n3", "n4"]);
    expect((await harness.snapshot(Step, context))?.done).toBe(4);
});

test("a write attached in a commit whose callback throws is never run", async () => {
    const { schema, app } = await schemasForTest();
    let now = 1_000_000;
    const { root, attached } = await open(schema, "pod-a", { now: () => now, ttlMs: 60_000 });
    let ran = false;

    const thrown = root.commit(async (tx) => {
        await attached.attach(tx, async (sql) => {
            ran = true;
            await sql.run(`INSERT INTO ${app}.note (id, body) VALUES (?, ?)`, "n1", "never");
        });
        throw new Error("the step changed its mind");
    }, context);
    await expect(thrown).rejects.toThrow("the step changed its mind");

    // The Session goes on, and the next commit stores its own write and not the abandoned one.
    await root.commit(async (tx) => {
        await attached.attach(tx, (sql) => sql.run(`INSERT INTO ${app}.note (id, body) VALUES (?, ?)`, "n2", "stored"));
    }, context);
    expect(ran).toBe(false);
    expect(await notes(app)).toEqual([{ id: "n2", body: "stored" }]);

    // The abandoned work is dropped once it is older than the time a work may wait.
    expect(attached.waiting).toBe(1);
    now += 60_001;
    await root.commit(async (tx) => {
        await attached.attach(tx, (sql) => sql.run(`INSERT INTO ${app}.note (id, body) VALUES (?, ?)`, "n3", "later"));
    }, context);
    expect(attached.waiting).toBe(0);
});

test("a commit whose attached write this storage does not have is rejected, not stored without it", async () => {
    const { schema, app } = await schemasForTest();
    const first = await open(schema);
    // Attached through an `AttachedWrites` of some other storage: this one cannot run it.
    const foreign = new AttachedWrites();

    const lost = first.root.commit(async (tx) => {
        await foreign.attach(tx, (sql) => sql.run(`INSERT INTO ${app}.note (id, body) VALUES (?, ?)`, "n1", "lost"));
        (await tx.doc(Step)).done = 1;
    }, context);
    await expect(lost).rejects.toBeInstanceOf(AttachedWritesMissing);

    await first.harness.close(context).catch(() => undefined);
    await first.storage.close(context).catch(() => undefined);
    const second = await reopen(schema);
    expect(await notes(app)).toEqual([]);
    expect((await second.harness.snapshot(Step, context))?.done ?? 0).toBe(0);
});

/** Runs the child until its second commit has written its row inside the transaction, then SIGKILLs it. */
const crashInsideCommit = (schema: string, app: string) =>
    new Promise<void>((resolve, reject) => {
        const child = fileURLToPath(new globalThis.URL("./fixtures/attached-child.ts", import.meta.url));
        const process_ = spawn(process.execPath, [child, schema, app, URL], { stdio: ["ignore", "pipe", "inherit"] });
        let output = "";
        process_.stdout.on("data", (chunk: Buffer) => {
            output += chunk.toString();
            if (output.includes("FIRST_COMMITTED") && output.includes("ROW_WRITTEN")) process_.kill("SIGKILL");
        });
        process_.on("exit", (_code, signal) =>
            signal === "SIGKILL" ? resolve() : reject(new Error(`the child ended without being killed mid-commit: ${output}`)),
        );
    });

test("a pod killed inside a commit leaves neither the row nor the checkpoint of that commit", async () => {
    const { schema, app } = await schemasForTest();

    await crashInsideCommit(schema, app);
    // pod-a's lease (1 s) runs out: nobody renews it any more.
    await sleep(1_500);
    const second = await reopen(schema);

    // The first commit is whole; of the second, neither half is there.
    expect(await notes(app)).toEqual([{ id: "n1", body: "committed" }]);
    expect((await second.harness.snapshot(Step, context))?.done).toBe(1);

    // The next owner makes the step again, and now both halves are stored.
    await second.root.commit(async (tx) => {
        await second.attached.attach(tx, (sql) =>
            sql.run(`INSERT INTO ${app}.note (id, body) VALUES (?, ?)`, "n2", "committed by the next pod"),
        );
        (await tx.doc(Step)).done = 2;
    }, context);
    expect(await notes(app)).toEqual([
        { id: "n1", body: "committed" },
        { id: "n2", body: "committed by the next pod" },
    ]);
    expect((await second.harness.snapshot(Step, context))?.done).toBe(2);
}, 60_000);
