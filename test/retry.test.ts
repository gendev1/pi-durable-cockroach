// What the conformance suite does not reach: CockroachDB aborting a commit's transaction with a serialization
// failure (40001), so the commit runs again. `inject_retry_errors_enabled` makes CockroachDB fail statements of an
// explicit transaction with 40001 a few times before letting it through.
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { createModels, fauxAssistantMessage, fauxProvider } from "@earendil-works/pi-ai";
import { AssistantEntry, createRegistry, Harness } from "@earendil-works/pi-durable";
import { afterEach, expect, test } from "vitest";
import { CockroachStorage } from "../src/storage.ts";
import { dropSchema, freshSchema, openDatabase } from "./helpers.ts";

const context = BACKGROUND_CONTEXT;
const schemas: string[] = [];

afterEach(async () => {
    for (const schema of schemas.splice(0)) await dropSchema(schema);
});

const harnessOn = async (schema: string, answers: string[], options: { injectRetries?: boolean } = {}) => {
    const db = await openDatabase();
    const storage = await CockroachStorage.open(db, { schema });
    // After the schema exists: DDL is not run in explicit transactions, so it would not be affected anyway.
    if (options.injectRetries) await db.exec("SET inject_retry_errors_enabled = true");
    const faux = fauxProvider();
    const models = createModels();
    models.setProvider(faux.provider);
    faux.setResponses(answers.map((answer) => fauxAssistantMessage(answer)));
    const harness = await Harness.open(storage, { models, registry: createRegistry() }, context);
    const root = await harness.root(context, {
        agent: { model: { provider: faux.getModel().provider, modelId: faux.getModel().id } },
    });
    return { harness, root, db };
};

const ask = async (root: Awaited<ReturnType<typeof harnessOn>>["root"], text: string) => {
    const settled = await (await root.submit({ type: "input", content: text }, context)).wait(context);
    if (settled.status !== "done" || settled.type !== "input") throw new Error(`not answered: ${JSON.stringify(settled)}`);
    const entry = await root.commit((tx) => tx.entry(AssistantEntry, settled.answer), context);
    return entry?.model?.[0]?.content;
};

const transcript = async (root: Awaited<ReturnType<typeof harnessOn>>["root"]) =>
    (await root.entries({ order: "ascending" }, 100, undefined, context)).items.map((entry) => entry.kind);

test("a conversation whose every commit is retried by CockroachDB is stored once", async () => {
    const schema = freshSchema();
    schemas.push(schema);
    const plain = freshSchema();
    schemas.push(plain);

    const retried = await harnessOn(schema, ["One.", "Two."], { injectRetries: true });
    expect(await ask(retried.root, "first")).toEqual([{ type: "text", text: "One." }]);
    expect(await ask(retried.root, "second")).toEqual([{ type: "text", text: "Two." }]);
    const withRetries = await transcript(retried.root);
    // The retry path really ran: CockroachDB failed the commits' statements and they were run again.
    expect(retried.db.retries).toBeGreaterThan(0);
    await retried.harness.close(context);

    // The same conversation without injected retries: the transcripts must be the same, nothing doubled.
    const reference = await harnessOn(plain, ["One.", "Two."]);
    await ask(reference.root, "first");
    await ask(reference.root, "second");
    expect(withRetries).toEqual(await transcript(reference.root));
    await reference.harness.close(context);
});

test("a reopened storage has everything the closed one committed", async () => {
    const schema = freshSchema();
    schemas.push(schema);

    const first = await harnessOn(schema, ["Hello."]);
    await ask(first.root, "hi");
    const before = await transcript(first.root);
    await first.harness.close(context);

    const second = await harnessOn(schema, []);
    expect(await transcript(second.root)).toEqual(before);
    expect(before).toContain("pi.assistant");
    await second.harness.close(context);
});
