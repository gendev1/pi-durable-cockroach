// One owner per storage: the lease and its fence.
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { createModels, fauxAssistantMessage, fauxProvider } from "@earendil-works/pi-ai";
import { createRegistry, Harness, type Storage } from "@earendil-works/pi-durable";
import { afterEach, expect, test } from "vitest";
import { LeaseHeld, LeaseLost } from "../src/lease.ts";
import { openCockroachStorage } from "../src/open.ts";
import { dropSchema, freshSchema, URL } from "./helpers.ts";

const context = BACKGROUND_CONTEXT;
const cleanup: Array<() => Promise<unknown>> = [];

afterEach(async () => {
    for (const step of cleanup.splice(0).reverse()) await step().catch(() => undefined);
});

const open = async (schema: string, owner: string, ttlMs = 15_000, onLost?: (error: LeaseLost) => void) => {
    const opened = await openCockroachStorage({ connection: URL, schema, owner, ttlMs, ...(onLost ? { onLost } : {}) });
    cleanup.push(() => opened.storage.close(context));
    return opened;
};

const schemaForTest = () => {
    const schema = freshSchema();
    cleanup.push(() => dropSchema(schema));
    return schema;
};

/** A Harness on `storage` whose model answers `answers`, and a function that asks its root conversation. */
const harnessOn = async (storage: Storage, answers: string[]) => {
    const faux = fauxProvider();
    const models = createModels();
    models.setProvider(faux.provider);
    faux.setResponses(answers.map((answer) => fauxAssistantMessage(answer)));
    const harness = await Harness.open(storage, { models, registry: createRegistry() }, context);
    const root = await harness.root(context, {
        agent: { model: { provider: faux.getModel().provider, modelId: faux.getModel().id } },
    });
    const ask = async (text: string) => (await root.submit({ type: "input", content: text }, context)).wait(context);
    return { harness, ask };
};

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

test("a second process cannot open a storage another one holds", async () => {
    const schema = schemaForTest();
    await open(schema, "pod-a");

    await expect(open(schema, "pod-b")).rejects.toBeInstanceOf(LeaseHeld);
});

test("a graceful close hands the storage to the next process at once", async () => {
    const schema = schemaForTest();
    const first = await open(schema, "pod-a");
    const { harness, ask } = await harnessOn(first.storage, ["Hello."]);
    expect((await ask("hi")).status).toBe("done");
    await harness.close(context);

    const second = await open(schema, "pod-b");
    expect(second.lease.epoch).toBe(2);
});

test("a process whose lease was taken over cannot commit anything", async () => {
    const schema = schemaForTest();
    // pod-a stalls (a long GC pause, a frozen node): its lease runs out without a renewal.
    const stale = await open(schema, "pod-a", 1_000);
    const before = await harnessOn(stale.storage, ["One.", "Two."]);
    expect((await before.ask("first")).status).toBe("done");
    stale.lease.abandon();
    await sleep(1_500);

    const fresh = await open(schema, "pod-b");
    expect(fresh.lease.epoch).toBe(2);

    // pod-a wakes up and tries to go on: its commit is refused by the fence, and nothing of it is stored.
    const rejected = await before.ask("second").then(
        () => undefined,
        (error: unknown) => error,
    );
    expect(String(rejected)).toMatch(/LeaseLost|lease was lost/);

    const after = await harnessOn(fresh.storage, []);
    const page = await after.harness.root(context).then((root) => root.entries({ order: "ascending" }, 100, undefined, context));
    const users = page.items.filter((entry) => entry.kind === "pi.user").length;
    expect(users).toBe(1);
});

test("a renewal that finds the lease taken reports it lost", async () => {
    const schema = schemaForTest();
    const lost: LeaseLost[] = [];
    const first = await open(schema, "pod-a", 600, (error) => lost.push(error));
    first.lease.abandon();
    await sleep(900);
    await open(schema, "pod-b");

    expect(await first.lease.renew()).toBe(false);
    expect(lost).toHaveLength(1);
});
