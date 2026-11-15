// Opening a storage the safe way, in this order:
//
//   connect ─► migrate the schema ─► acquire the lease ─► open the storage (reads next_id) ─► Harness.open
//
// The lease comes before the storage reads its metadata, so a previous owner that was still committing cannot
// leave this process with a stale next ID. At close: the Harness's last commit ─► lease released ─► disconnect.
import { AttachedWrites, type AttachedWritesOptions } from "./attached.ts";
import { CockroachDatabase, type CockroachDatabaseOptions } from "./database.ts";
import { Lease, type LeaseOptions } from "./lease.ts";
import { createCockroachSchema, migrate } from "./schema.ts";
import { CockroachStorage } from "./storage.ts";

export interface OpenCockroachStorageOptions extends CockroachDatabaseOptions, LeaseOptions {
    /** The storage's schema, e.g. `ego_0`. */
    readonly schema: string;
    /** Settings of the application's attached writes (`attached.ts`). */
    readonly attached?: AttachedWritesOptions;
}

export async function openCockroachStorage(options: OpenCockroachStorageOptions) {
    const database = await CockroachDatabase.open(options);
    try {
        const schema = createCockroachSchema(options.schema);
        await migrate(database, schema);
        const lease = new Lease(database, schema, options);
        await lease.acquire();
        const attached = new AttachedWrites(options.attached);
        const storage = await CockroachStorage.open(database, {
            schema: options.schema,
            fence: lease.fence,
            onClose: () => lease.release(),
            attached,
        });
        return { storage, lease, database, attached };
    } catch (error) {
        await database.close().catch(() => undefined);
        throw error;
    }
}
