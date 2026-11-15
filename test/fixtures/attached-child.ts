// The process the attached-writes crash test kills. It stores one commit with a row, then starts a second commit
// whose attached write inserts its row and never returns: the process is killed there, inside the commit's
// transaction, after the row was written and before anything was committed.
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { createModels, fauxProvider } from "@earendil-works/pi-ai";
import { createRegistry, defineDoc, Harness } from "@earendil-works/pi-durable";
import { openCockroachStorage } from "../../src/open.ts";

const [schema, app, url] = process.argv.slice(2);
const context = BACKGROUND_CONTEXT;
const Step = defineDoc<{ done: number }>({
    kind: "test.step",
    version: 1,
    scope: "session",
    initial: () => ({ done: 0 }),
    checkpointWhen: () => false,
});
const { storage, attached } = await openCockroachStorage({ connection: url!, schema: schema!, owner: "pod-a", ttlMs: 1_000 });
const models = createModels();
models.setProvider(fauxProvider().provider);
const harness = await Harness.open(storage, { models, registry: createRegistry() }, context);
const root = await harness.root(context);

await root.commit(async (tx) => {
    await attached.attach(tx, (sql) => sql.run(`INSERT INTO ${app}.note (id, body) VALUES (?, ?)`, "n1", "committed"));
    (await tx.doc(Step)).done = 1;
}, context);
console.log("FIRST_COMMITTED");

await root.commit(async (tx) => {
    await attached.attach(tx, async (sql) => {
        await sql.run(`INSERT INTO ${app}.note (id, body) VALUES (?, ?)`, "n2", "written, never committed");
        console.log("ROW_WRITTEN");
        await new Promise(() => {});
    });
    (await tx.doc(Step)).done = 2;
}, context);
