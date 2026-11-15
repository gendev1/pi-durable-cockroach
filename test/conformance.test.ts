// pi-durable's own storage conformance suite, against CockroachDB.
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { registerStorageConformance } from "@earendil-works/pi-durable/testing";
import { describe, expect, it } from "vitest";
import { CockroachStorage } from "../src/storage.ts";
import { dropSchema, freshSchema, openDatabase } from "./helpers.ts";

registerStorageConformance({ describe, expect, it: (name, test) => it(name, test, 60_000) }, "CockroachStorage", async (use) => {
    const schema = freshSchema();
    try {
        const storage = await CockroachStorage.open(await openDatabase(), { schema });
        try {
            await use(storage);
        } finally {
            await storage.close(BACKGROUND_CONTEXT).catch(() => undefined);
        }
    } finally {
        // Also when opening failed half-way: the schema may exist.
        await dropSchema(schema);
    }
});
