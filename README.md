# pi-durable-cockroach

CockroachDB storage for [`@earendil-works/pi-durable`](https://www.npmjs.com/package/@earendil-works/pi-durable) 1.5.2. It ports pi-durable's own SQLite storage (pi `v1.5.2`, `packages/durable/src/storage/sqlite`, MIT) to CockroachDB, and adds a lease so that only one process owns a storage at a time.

pi-durable itself is not changed or forked. The package implements its public `Storage` interface and passes its storage conformance suite.

```ts
import { openCockroachStorage } from "pi-durable-cockroach";

const { storage } = await openCockroachStorage({
    connection: process.env.COCKROACH_URL!, // postgresql://user@host:26257/db?sslmode=verify-full
    schema: "ego_0",                         // one CockroachDB schema per storage
    owner: process.env.POD_NAME!,            // who holds the lease
    onLost: () => process.exit(1),           // another pod took over: stop
});
const harness = await Harness.open(storage, { models, registry }, context);
harness.resume();
```

## Attached writes: the application's rows in the commit's transaction

An application on this storage usually keeps tables of its own in the same CockroachDB database. A task step that
writes such a table and then stores its checkpoint would make two transactions, and a crash between them would
leave one without the other. `attach` joins them: the statements run inside the transaction of the pi-durable commit
they belong to, so the rows and the checkpoint are stored together or not at all.

```ts
const { storage, attached } = await openCockroachStorage({ connection, schema: "ego_0", owner });

await runtime.commit(async (tx) => {
    await attached.attach(tx, (sql) => sql.run("INSERT INTO app.note (id, body) VALUES (?, ?)", id, body));
    return { status: "running", checkpoint: { phase: "next" } };
}, context);
```

It works in any commit that has a `Tx`: a task's `runtime.commit`, a tool's `api.commit`, a conversation's `commit`.

- **How a write finds its commit.** pi-durable's `Tx` has no place for SQL, and `Storage.commit` gets several
  commits in one call. `attach` writes a random token into a small Session document through the `Tx`, and keeps the
  work in memory under that token. The storage finds the token in the batch that carries it and runs the work
  there, after the batch passed its checks. pi-durable itself is not changed.
- **The work may run more than once.** CockroachDB reruns a transaction after a serialization failure. The work
  must do nothing but SQL through the executor it is given.
- **A failing statement rejects the commit, and nothing is stored.** pi-durable fails a Session whose commit was
  rejected, so the application reopens it. Check what can be checked before the commit; write
  `ON CONFLICT DO NOTHING` where a row may already be there.
- **A commit whose callback throws never reaches the storage**, so its work never runs. It is dropped from memory
  after `attached.ttlMs` (default ten minutes); `attached.waiting` counts what waits.
- **A commit that carries a token this storage does not know is rejected** (`AttachedWritesMissing`), because
  storing it without its writes is what attaching is there to prevent.
- **Put the application's tables in a schema of their own**, not in the storage's schema.
- **What stays outside:** anything that is not SQL in this database. A mail, a call to another service or to a
  model cannot join the transaction and still needs its own key against happening twice.
- Statements go through the storage's one connection, one at a time. Reads that need not be in the commit should
  use a connection of the application's own.

## What was ported, and what changed

| File | Status |
|---|---|
| `storage.ts`, `commit.ts`, `rows.ts`, `scan.ts`, `ids.ts` | Upstream logic, unchanged. Only imports and SQL text differ: `json_each(?)`/`json_extract` became `= ANY(?)`/`unnest(?)`, and `"family"` is quoted |
| `storage.ts` `commit()` | **One logic change:** the state of each call is reset inside the transaction callback, because CockroachDB may run the callback again. The fence runs first in the same transaction |
| `statements.ts` | Rewritten: multi-row `VALUES` with bound cells, in chunks under PostgreSQL's parameter limit |
| `schema.ts` | Upstream's schema after its two migrations, in CockroachDB types. JSON stays `STRING`, not `JSONB`, so records read back exactly as written |
| `database.ts` | New. One `pg` connection and a queue (the facade requires one operation at a time). `?`→`$n`. INT8 read as a safe number. CockroachDB's `cockroach_restart` retry protocol on `40001`. `40003` (ambiguous commit) is never retried |
| `lease.ts`, `open.ts` | New. The lease and fencing token, and the safe open order |
| `attached.ts` | New. The application's writes that run in a commit's transaction (above). `storage.ts` `commit()` calls it for each batch it accepts: the second logic change to the ported file |

**Lease.** The `leases` row holds `(owner, epoch, expires_at)` and is compared with CockroachDB's clock.
- `acquire` takes the row if it is free, expired or already ours, and increments the epoch.
- Every commit's transaction first checks that the row is still ours, at our epoch, and not expired.
- Under SERIALIZABLE, a stalled pod that wakes up after a takeover gets its commit retried. The retry sees the new epoch, and the commit fails with `LeaseLost` without writing anything.
- The lease renews every ttl/3. A graceful close releases it, so the next pod takes over at once.

**Open order:** connect → migrate → acquire the lease → read `next_id` → `Harness.open`. The lease comes before the metadata read, so a previous owner can't leave this process with a stale next ID.

## Tests

`npm run db:up` starts `cockroachdb/cockroach:latest` (v26.4.0 when this was written) in single-node mode. `npm run check` then runs 47 tests:

- **pi-durable's storage conformance suite:** all 32 cases.
- **Retries:** with `inject_retry_errors_enabled`, every commit is retried. The test checks that the retry path really ran (`database.retries > 0`) and that the transcript matches a run without retries.
- **Reopen:** a reopened storage has everything that was committed.
- **Lease:** a second owner is refused. A graceful close hands over at once. A stale owner can't commit, and nothing of its attempt is stored. A lost renewal calls `onLost`.
- **Crash:** a child process is `SIGKILL`ed inside a model call. Another owner takes over after the TTL, and the answer arrives once. The same `requestId` maps to the same submission.
- **Attached writes:** a row is stored with its commit. A failing statement leaves neither the row nor the commit. A transaction restarted after the row was written (`crdb_internal.force_retry`) runs the work twice and stores one row. Several works in one commit run in order, and four commits made at once each store their own. A work whose commit's callback throws never runs. A commit with a token of another storage is rejected. A child process is `SIGKILL`ed inside a commit after its row was written: the next owner finds neither the row nor the checkpoint, and stores both when it makes the step again.

## Using it from another package

Install it as a **copy**, not a symlink: `install-links=true` in the consumer's `.npmrc`, or a published version. pi-durable's Session tells a bad request from a broken storage with `instanceof StorageRequestError`. A symlinked package would load its own pi-durable, that check would fail, and one bad read would end the whole session. `ego/test/install.test.ts` guards this.

## Not done yet

- **Attached writes, open points:** a failing statement fails the whole Session (that is pi-durable's rule for a rejected commit), so a write that can fail for an ordinary reason must be checked before the commit. The token is found by a key in a document's whole value; if pi-durable ever rewrote that document's value on its own, after a restart, the storage would reject that commit as carrying unknown writes. Not seen, and not ruled out by reading pi-durable's code. Only tested with one Harness on one node.

- **Upgrades:** upstream's schema and queries are copied, so each pi-durable upgrade needs a diff of `packages/durable/src/storage/sqlite` against `v1.5.2` and the conformance suite rerun. The unreleased signals on pi `main` (after 1.5.2) change task records only, which are stored as JSON, not the tables.
- **Hot ranges:** primary keys are the storage's own increasing IDs. With one writer per schema that's fine. At very high write rates, consider `USING HASH` on `entries` and `document_revisions`.
- **Not tested** on a multi-node or multi-region cluster, or on CockroachDB Cloud.
