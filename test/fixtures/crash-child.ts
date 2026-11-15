// The process the crash test kills: it opens the storage as `pod-a`, submits one input, and hangs inside the model
// call. It prints `SUBMITTED <id>` and then `MODEL_CALLED` so the test knows when to kill it.
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { createModels, fauxProvider } from "@earendil-works/pi-ai";
import { createRegistry, Harness } from "@earendil-works/pi-durable";
import { openCockroachStorage } from "../../src/open.ts";

const [schema, url] = process.argv.slice(2);
const context = BACKGROUND_CONTEXT;
const { storage } = await openCockroachStorage({ connection: url!, schema: schema!, owner: "pod-a", ttlMs: 1_000 });
const faux = fauxProvider();
const models = createModels();
models.setProvider(faux.provider);
faux.setResponses([
    () => {
        console.log("MODEL_CALLED");
        return new Promise(() => {});
    },
]);
const harness = await Harness.open(storage, { models, registry: createRegistry() }, context);
const root = await harness.root(context, {
    agent: { model: { provider: faux.getModel().provider, modelId: faux.getModel().id } },
});
const submission = await root.submit({ type: "input", content: "Are you there?", requestId: "q1" }, context);
console.log(`SUBMITTED ${submission.id}`);
await new Promise(() => {});
