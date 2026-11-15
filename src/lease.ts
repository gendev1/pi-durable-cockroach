// One owner per storage. pi-durable has no cross-process locking ("one process owns a storage at a time"), so two
// pods on one storage would interleave commits. The lease is one row in the storage's schema:
//
//   leases(owner, epoch, expires_at)
//
// - acquire: in one transaction, take the row if it is free, expired, or ours; the epoch goes up by one.
// - fence:   every commit's transaction first reads the row and refuses unless it is still ours, at our epoch, and
//            not expired. Under SERIALIZABLE, a takeover that writes the row and a commit that read it cannot both
//            commit as if the other had not happened: the stale owner's commit is retried, sees the new epoch, and
//            fails with LeaseLost. Nothing of it is written.
// - renew:   every ttl/3, push expires_at forward if the row is still ours; if not, `onLost` runs.
// - release: on a graceful shutdown, expire the row now, so the next pod takes it at once.
//
// Expiry is compared with CockroachDB's clock (`now()`), never a pod's, so clock skew between pods does not matter.
import type { Database, Executor } from "./database.ts";
import type { CockroachSchema } from "./schema.ts";

export class LeaseHeld extends Error {
    readonly owner: string;
    readonly expiresAt: Date;

    constructor(owner: string, expiresAt: Date) {
        super(`The storage is leased to ${owner} until ${expiresAt.toISOString()}`);
        this.name = "LeaseHeld";
        this.owner = owner;
        this.expiresAt = expiresAt;
    }
}

export class LeaseLost extends Error {
    constructor(reason: string) {
        super(`The storage lease was lost: ${reason}`);
        this.name = "LeaseLost";
    }
}

export interface LeaseOptions {
    /** Who holds it: a pod name, unique among the processes that may open the storage. */
    readonly owner: string;
    /** How long the lease lasts without a renewal. Default 15 s. */
    readonly ttlMs?: number;
    /** Runs once when a renewal finds the lease gone. The host should stop: close the Harness and exit. */
    readonly onLost?: (error: LeaseLost) => void;
}

type LeaseRow = { owner: string; epoch: number; expires_at: Date; valid: boolean };

export class Lease {
    readonly #db: Database;
    readonly #table: string;
    readonly #owner: string;
    readonly #ttlMs: number;
    readonly #onLost: ((error: LeaseLost) => void) | undefined;
    #epoch: number | undefined;
    #timer: ReturnType<typeof setInterval> | undefined;
    #lost = false;

    constructor(db: Database, schema: CockroachSchema, options: LeaseOptions) {
        if (options.owner.trim() === "") throw new Error("A lease needs an owner");
        this.#db = db;
        this.#table = schema.tables.leases;
        this.#owner = options.owner;
        this.#ttlMs = options.ttlMs ?? 15_000;
        this.#onLost = options.onLost;
    }

    get owner(): string {
        return this.#owner;
    }

    /** The epoch this process holds, or undefined before `acquire`. */
    get epoch(): number | undefined {
        return this.#epoch;
    }

    /** Takes the lease or throws LeaseHeld; then renews it until `release`. */
    async acquire(): Promise<void> {
        const interval = `${this.#ttlMs} milliseconds`;
        this.#epoch = await this.#db.transaction(async (tx) => {
            const row = await tx.get<LeaseRow>(
                `SELECT owner, epoch, expires_at, expires_at > now() AS valid FROM ${this.#table} WHERE singleton = 1`,
            );
            if (row !== undefined && row.valid && row.owner !== this.#owner) {
                throw new LeaseHeld(row.owner, row.expires_at);
            }
            const epoch = (row?.epoch ?? 0) + 1;
            await tx.run(
                `UPSERT INTO ${this.#table} (singleton, owner, epoch, expires_at) VALUES (1, ?, ?, now() + ?::INTERVAL)`,
                this.#owner,
                epoch,
                interval,
            );
            return epoch;
        });
        this.#lost = false;
        this.#timer = setInterval(() => void this.renew(), Math.max(100, Math.floor(this.#ttlMs / 3)));
        this.#timer.unref?.();
    }

    /** The check of every commit: throws LeaseLost unless the lease is still ours, at our epoch, and unexpired. */
    readonly fence = async (tx: Executor): Promise<void> => {
        if (this.#epoch === undefined) throw new LeaseLost("it was never acquired");
        const row = await tx.get<LeaseRow>(
            `SELECT owner, epoch, expires_at, expires_at > now() AS valid FROM ${this.#table} WHERE singleton = 1`,
        );
        if (row === undefined) throw new LeaseLost("the lease row is gone");
        if (row.owner !== this.#owner || row.epoch !== this.#epoch) {
            throw new LeaseLost(`${row.owner} holds epoch ${row.epoch}, this process had ${this.#epoch}`);
        }
        if (!row.valid) throw new LeaseLost(`it expired at ${row.expires_at.toISOString()}`);
    };

    /** Pushes the expiry forward; answers whether the lease is still ours. Called by the timer. */
    async renew(): Promise<boolean> {
        if (this.#epoch === undefined || this.#lost) return false;
        let renewed: boolean;
        try {
            renewed =
                (await this.#db.runCounted(
                    `UPDATE ${this.#table} SET expires_at = now() + ?::INTERVAL
                        WHERE singleton = 1 AND owner = ? AND epoch = ? AND expires_at > now()`,
                    `${this.#ttlMs} milliseconds`,
                    this.#owner,
                    this.#epoch,
                )) === 1;
        } catch {
            // A failed renewal is not a lost lease yet: the next one may succeed before it expires; the fence holds.
            return false;
        }
        if (!renewed) this.#lose(new LeaseLost("a renewal found it taken or expired"));
        return renewed;
    }

    /** Stops renewing and expires the lease now, if it is still ours, so the next owner need not wait. */
    async release(): Promise<void> {
        this.#stop();
        if (this.#epoch === undefined) return;
        await this.#db.run(
            `UPDATE ${this.#table} SET expires_at = now() WHERE singleton = 1 AND owner = ? AND epoch = ?`,
            this.#owner,
            this.#epoch,
        );
        this.#epoch = undefined;
    }

    /** Stops renewing without touching the row: what a crashed process leaves (for tests). */
    abandon(): void {
        this.#stop();
    }

    #lose(error: LeaseLost): void {
        if (this.#lost) return;
        this.#lost = true;
        this.#stop();
        this.#onLost?.(error);
    }

    #stop(): void {
        if (this.#timer !== undefined) clearInterval(this.#timer);
        this.#timer = undefined;
    }
}
