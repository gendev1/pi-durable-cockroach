// The database under the storage: one `pg` connection to CockroachDB, used by one operation at a time, as
// pi-durable's SQLite facade requires ("adapters must queue unrelated operations and other transactions until the
// transaction finishes"). Not ported: the facade's shape follows `@earendil-works/chord/sqlite`, its SQL is
// CockroachDB's.
//
// - SQL text uses `?` placeholders like the ported storage; they become `$1, $2, ...` here. The storage's SQL has no
//   `?` inside a string literal.
// - INT8 columns come back as JavaScript numbers. Every value the storage keeps in one is a safe integer (IDs, seqs,
//   versions, 0/1 flags); a larger one throws instead of losing precision.
// - A transaction CockroachDB aborts with a serialization failure (SQLSTATE 40001, also raised at commit) is run
//   again, with backoff, through CockroachDB's `cockroach_restart` savepoint protocol. The callback must therefore be
//   repeatable: it may keep state only inside itself.
//   An ambiguous result (40003: the commit may or may not have happened) is never retried: it rejects, as
//   pi-durable's `Storage.commit` contract requires for an unknown outcome.
import pg from "pg";

/** A value bound to a placeholder: what the storage binds, and arrays for `= ANY(?)` and `unnest(?)`. */
export type SqlValue = null | number | bigint | string | boolean | readonly (string | number | null)[];

/** The operations of the database and of a transaction handle. */
export interface Executor {
    exec(sql: string): Promise<void>;
    run(sql: string, ...params: SqlValue[]): Promise<void>;
    /** Runs one statement; answers how many rows it inserted, updated or deleted. */
    runCounted(sql: string, ...params: SqlValue[]): Promise<number>;
    get<T extends object>(sql: string, ...params: SqlValue[]): Promise<T | undefined>;
    all<T extends object>(sql: string, ...params: SqlValue[]): Promise<T[]>;
}

export interface Database extends Executor {
    /** Runs `callback` in one SERIALIZABLE transaction, retried on CockroachDB's retryable errors. */
    transaction<T>(callback: (transaction: Executor) => Promise<T>): Promise<T>;
    close(): Promise<void>;
}

export interface CockroachDatabaseOptions {
    /** `postgresql://user@host:26257/db?sslmode=...`, or the `pg` client settings. */
    readonly connection: string | pg.ClientConfig;
    /** How many times a transaction is tried before its last error is thrown. Default 12. */
    readonly maxAttempts?: number;
}

const INT8_OID = 20;

const parseInt8 = (value: string): number => {
    const number = Number(value);
    if (!Number.isSafeInteger(number)) throw new RangeError(`INT8 value ${value} is not a safe integer`);
    return number;
};

/** The type parsers of this connection: INT8 as a number, everything else as `pg` does. */
const types = {
    getTypeParser: ((oid: number, format?: "text" | "binary") =>
        oid === INT8_OID && format !== "binary"
            ? parseInt8
            : pg.types.getTypeParser(oid, format as "text")) as pg.CustomTypesConfig["getTypeParser"],
};

/** `?` placeholders as `$1, $2, ...`. */
export const numbered = (sql: string): string => {
    let index = 0;
    return sql.replaceAll("?", () => `$${++index}`);
};

const RETRYABLE = new Set(["40001"]);

export const isRetryable = (error: unknown): boolean =>
    typeof error === "object" && error !== null && RETRYABLE.has((error as { code?: string }).code ?? "");

const executorOf = (client: pg.Client): Executor => {
    const query = (sql: string, params: SqlValue[]) => client.query(numbered(sql), params as unknown[]);
    return {
        exec: async (sql) => {
            await client.query(sql);
        },
        run: async (sql, ...params) => {
            await query(sql, params);
        },
        runCounted: async (sql, ...params) => (await query(sql, params)).rowCount ?? 0,
        get: async <T extends object>(sql: string, ...params: SqlValue[]) =>
            (await query(sql, params)).rows[0] as T | undefined,
        all: async <T extends object>(sql: string, ...params: SqlValue[]) => (await query(sql, params)).rows as T[],
    };
};

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export class CockroachDatabase implements Database {
    readonly #client: pg.Client;
    readonly #executor: Executor;
    readonly #maxAttempts: number;
    /** The tail of the queue: every operation waits for the one before it. */
    #tail: Promise<unknown> = Promise.resolve();
    #closed = false;
    #retries = 0;

    private constructor(client: pg.Client, maxAttempts: number) {
        this.#client = client;
        this.#executor = executorOf(client);
        this.#maxAttempts = maxAttempts;
    }

    static async open(options: CockroachDatabaseOptions): Promise<CockroachDatabase> {
        const config = typeof options.connection === "string" ? { connectionString: options.connection } : options.connection;
        const client = new pg.Client({ ...config, types });
        await client.connect();
        return new CockroachDatabase(client, options.maxAttempts ?? 12);
    }

    /** Runs `operation` after every operation queued before it. */
    #queued<T>(operation: () => Promise<T>): Promise<T> {
        if (this.#closed) return Promise.reject(new Error("CockroachDatabase is closed"));
        const result = this.#tail.then(operation);
        this.#tail = result.catch(() => undefined);
        return result;
    }

    exec(sql: string) {
        return this.#queued(() => this.#executor.exec(sql));
    }
    run(sql: string, ...params: SqlValue[]) {
        return this.#queued(() => this.#executor.run(sql, ...params));
    }
    runCounted(sql: string, ...params: SqlValue[]) {
        return this.#queued(() => this.#executor.runCounted(sql, ...params));
    }
    get<T extends object>(sql: string, ...params: SqlValue[]) {
        return this.#queued(() => this.#executor.get<T>(sql, ...params));
    }
    all<T extends object>(sql: string, ...params: SqlValue[]) {
        return this.#queued(() => this.#executor.all<T>(sql, ...params));
    }

    /**
     * CockroachDB's client-side retry protocol: `SAVEPOINT cockroach_restart` after BEGIN, `RELEASE` it to commit,
     * and on a 40001 `ROLLBACK TO` it and run the callback again in the same transaction. A retried transaction
     * keeps its priority, so it is not starved by newer ones.
     */
    transaction<T>(callback: (transaction: Executor) => Promise<T>): Promise<T> {
        return this.#queued(async () => {
            const client = this.#client;
            await client.query("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE");
            await client.query("SAVEPOINT cockroach_restart");
            for (let attempt = 1; ; attempt++) {
                let live = true;
                // The handle refuses work once its attempt is over, as the facade requires.
                const guard =
                    <A extends unknown[], R>(call: (...args: A) => Promise<R>) =>
                    (...args: A): Promise<R> =>
                        live ? call(...args) : Promise.reject(new Error("The transaction handle has expired"));
                const handle: Executor = {
                    exec: guard(this.#executor.exec),
                    run: guard(this.#executor.run),
                    runCounted: guard(this.#executor.runCounted),
                    get: guard(this.#executor.get) as Executor["get"],
                    all: guard(this.#executor.all) as Executor["all"],
                };
                try {
                    const result = await callback(handle);
                    // RELEASE commits; it may still fail with 40001, which is retried like any other.
                    await client.query("RELEASE SAVEPOINT cockroach_restart");
                    await client.query("COMMIT");
                    return result;
                } catch (error) {
                    if (isRetryable(error) && attempt < this.#maxAttempts) {
                        const restarted = await client.query("ROLLBACK TO SAVEPOINT cockroach_restart").then(
                            () => true,
                            () => false,
                        );
                        if (restarted) {
                            this.#retries++;
                            await sleep(Math.min(1_000, 5 * 2 ** attempt) * (0.5 + Math.random() / 2));
                            continue;
                        }
                    }
                    try {
                        await client.query("ROLLBACK");
                    } catch (rollback) {
                        throw new AggregateError([error, rollback], "The transaction failed and its rollback failed");
                    }
                    throw error;
                } finally {
                    live = false;
                }
            }
        });
    }

    /** How many times a transaction was run again after a serialization failure: a metric worth exporting. */
    get retries(): number {
        return this.#retries;
    }

    async close(): Promise<void> {
        if (this.#closed) return;
        const last = this.#tail;
        this.#closed = true;
        await last;
        await this.#client.end();
    }
}
