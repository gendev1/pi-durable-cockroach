// Writes of the application's own, run in the transaction of one pi-durable commit. Not ported: upstream has nothing
// like it, because its SQLite storage owns its file alone.
//
// An application on this storage usually keeps tables of its own in the same CockroachDB database. A task step that
// writes such a table and then stores its checkpoint makes two transactions, and a crash between them leaves one
// without the other. `attach` joins them: the application's statements run inside the commit's transaction, so the
// rows and the checkpoint are stored together or not at all.
//
//   runtime.commit(async (tx) => {
//       await attached.attach(tx, (sql) => sql.run("INSERT INTO app.note (id, body) VALUES (?, ?)", id, body));
//       return { status: "running", checkpoint: { phase: "next" } };
//   }, context);
//
// How a write finds its commit. pi-durable's `Tx` has no place for SQL, and `Storage.commit` gets several commits
// (batches) in one call, so the work must be tied to one batch. `attach` writes a random token into a small Session
// document through the `Tx` itself, and keeps the work in memory under that token. The document always stores its
// whole value (`checkpointWhen: () => true`), so the storage finds the token in the batch that carries it, runs the
// work there, and nowhere else. A commit whose callback throws never reaches the storage, and its work never runs.
//
// What the caller must know:
// - The work may run more than once: CockroachDB reruns a transaction after a serialization failure. It must do
//   nothing but SQL through the executor it is given, and must not keep results of an earlier run.
// - A statement that fails rejects the commit, and pi-durable fails the Session on a rejected commit (reopen it).
//   Nothing is stored. So check what can be checked before the commit, and write `ON CONFLICT DO NOTHING` where a
//   row may already be there.
// - The work is not kept across a restart. A commit is not either: after a crash the task step runs again and
//   attaches again.
import { randomUUID } from "node:crypto";
import { defineDocFamily, type StorageWrite, type Tx } from "@earendil-works/pi-durable";
import { type Executor, isRetryable } from "./database.ts";

/** The key of the token in the marker document's value. */
export const ATTACHED_KEY = "pi-durable-cockroach:attached";

/** Statements of the application, run through the commit's transaction. It may run again: see the file's head. */
export type AttachedWork = (sql: Executor) => Promise<void>;

/** A statement of an attached work failed. The commit was rejected and nothing of it is stored. */
export class AttachedWritesFailed extends Error {
    constructor(cause: unknown) {
        super(`An attached write failed, so its commit was rejected: ${cause instanceof Error ? cause.message : String(cause)}`, { cause });
        this.name = "AttachedWritesFailed";
    }
}

/**
 * A commit carries the token of a work this storage does not have: it was attached through another storage's
 * `AttachedWrites`, or more than `ttlMs` before its commit reached the storage. The commit was rejected, because
 * storing it without its writes is what attaching is there to prevent.
 */
export class AttachedWritesMissing extends Error {
    constructor(token: string) {
        super(`A commit carries attached writes this storage does not have (token ${token})`);
        this.name = "AttachedWritesMissing";
    }
}

type Marker = { [ATTACHED_KEY]: string };

/**
 * The document that carries the token. One member for each slot; a whole value at every change, so that the storage
 * reads the token from the write itself and needs no lookup.
 */
const MarkerDoc = defineDocFamily<Marker, null>({
    kind: "pi-durable-cockroach.attached",
    version: 1,
    scope: "session",
    family: true,
    initial: () => ({ [ATTACHED_KEY]: "" }),
    checkpointWhen: () => true,
});

/** The tokens of the attached works a batch carries, in the order of its writes. */
export const attachedTokens = (writes: readonly StorageWrite[]): string[] =>
    writes.flatMap((write) => {
        if (write.type !== "document.create" && write.type !== "document.change") return [];
        if (write.content.kind !== "base") return [];
        const token = write.content.value[ATTACHED_KEY];
        return typeof token === "string" && token !== "" ? [token] : [];
    });

export interface AttachedWritesOptions {
    /** How long a work waits for its commit before it is dropped. Default ten minutes. */
    readonly ttlMs?: number;
    readonly now?: () => number;
}

/** The works that wait for their commits. One for each storage: `openCockroachStorage` makes it. */
export class AttachedWrites {
    readonly #staged = new Map<string, { readonly works: AttachedWork[]; readonly at: number }>();
    /** The token each open transaction already has for a slot, so that a second `attach` joins the first. */
    readonly #open = new WeakMap<Tx, Map<string, string>>();
    readonly #ttlMs: number;
    readonly #now: () => number;

    constructor(options: AttachedWritesOptions = {}) {
        this.#ttlMs = options.ttlMs ?? 600_000;
        this.#now = options.now ?? Date.now;
    }

    /**
     * Run `work` in the transaction that stores `tx`'s commit. Several works attached to one commit run in the order
     * they were attached.
     *
     * `slot` names the marker document. Commits of one Session are stored one after another, so one slot serves them
     * all; give a slot of its own (a task's or a conversation's ID) to a part of the application whose commits should
     * not all rewrite the same small document.
     */
    async attach(tx: Tx, work: AttachedWork, slot = "default"): Promise<void> {
        this.#dropExpired();
        const slots = this.#open.get(tx) ?? new Map<string, string>();
        this.#open.set(tx, slots);
        const known = slots.get(slot);
        if (known !== undefined) {
            this.#staged.get(known)?.works.push(work);
            return;
        }
        const token = randomUUID();
        this.#staged.set(token, { works: [work], at: this.#now() });
        slots.set(slot, token);
        (await tx.doc(MarkerDoc, slot, null))[ATTACHED_KEY] = token;
    }

    /** How many works wait for a commit: a number worth watching, since it should return to zero. */
    get waiting(): number {
        return this.#staged.size;
    }

    /**
     * Called by the storage inside a commit's transaction, for one accepted batch: run the works of `tokens`. A
     * serialization failure passes through as it is, so that the transaction is run again.
     */
    async run(tokens: readonly string[], transaction: Executor): Promise<void> {
        for (const token of tokens) {
            const staged = this.#staged.get(token);
            if (staged === undefined) throw new AttachedWritesMissing(token);
            for (const work of staged.works) {
                try {
                    await work(transaction);
                } catch (error) {
                    throw isRetryable(error) ? error : new AttachedWritesFailed(error);
                }
            }
        }
    }

    /** Called by the storage once a commit call is over, stored or not: its works are done with. */
    settle(tokens: readonly string[]): void {
        for (const token of tokens) this.#staged.delete(token);
    }

    #dropExpired(): void {
        const before = this.#now() - this.#ttlMs;
        for (const [token, staged] of this.#staged) if (staged.at < before) this.#staged.delete(token);
    }
}
