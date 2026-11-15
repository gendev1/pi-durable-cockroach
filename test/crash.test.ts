// A pod killed in the middle of a model call: the next pod takes the storage over and the answer arrives, once.
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { createModels, fauxAssistantMessage, fauxProvider } from "@earendil-works/pi-ai";
import { createRegistry, Harness, type SubmissionId } from "@earendil-works/pi-durable";
import { afterEach, expect, test } from "vitest";
import { openCockroachStorage } from "../src/open.ts";
import { dropSchema, freshSchema, URL as DATABASE_URL } from "./helpers.ts";

const context = BACKGROUND_CONTEXT;
const child = fileURLToPath(new URL("./fixtures/crash-child.ts", import.meta.url));
const schemas: string[] = [];

afterEach(async () => {
    for (const schema of schemas.splice(0)) await dropSchema(schema);
});

/** Runs the child until it is inside the model call, then SIGKILLs it. Answers the submission it made. */
const crashMidModelCall = (schema: string) =>
    new Promise<SubmissionId>((resolve, reject) => {
        const process_ = spawn(process.execPath, [child, schema, DATABASE_URL], { stdio: ["ignore", "pipe", "inherit"] });
        let submitted: number | undefined;
        let output = "";
        process_.stdout.on("data", (chunk: Buffer) => {
            output += chunk.toString();
            submitted ??= Number(/SUBMITTED (\d+)/.exec(output)?.[1]) || undefined;
            if (submitted !== undefined && output.includes("MODEL_CALLED")) process_.kill("SIGKILL");
        });
        process_.on("exit", (_code, signal) =>
            signal === "SIGKILL" && submitted !== undefined
                ? resolve(submitted as SubmissionId)
                : reject(new Error(`the child ended without being killed mid-call: ${output}`)),
        );
    });

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

test("a pod killed during a model call is taken over and the answer arrives once", async () => {
    const schema = freshSchema();
    schemas.push(schema);

    const submissionId = await crashMidModelCall(schema);
    // pod-a's lease (1 s) runs out: nobody renews it any more.
    await sleep(1_500);

    const { storage, lease } = await openCockroachStorage({ connection: DATABASE_URL, schema, owner: "pod-b" });
    expect(lease.epoch).toBe(2);
    const faux = fauxProvider();
    const models = createModels();
    models.setProvider(faux.provider);
    faux.setResponses([fauxAssistantMessage("Yes, still here.")]);
    const harness = await Harness.open(storage, { models, registry: createRegistry() }, context);
    harness.resume();

    const submission = await harness.submission(submissionId, context);
    const settled = await submission!.wait(context);
    expect(settled.status).toBe("done");

    const root = await harness.root(context);
    const kinds = (await root.entries({ order: "ascending" }, 100, undefined, context)).items.map((entry) => entry.kind);
    expect(kinds.filter((kind) => kind === "pi.user")).toHaveLength(1);
    expect(kinds.filter((kind) => kind === "pi.assistant")).toHaveLength(1);

    // The same request after the crash is the same submission, not a second question.
    const again = await root.submit({ type: "input", content: "Are you there?", requestId: "q1" }, context);
    expect(again.id).toBe(submissionId);

    await harness.close(context);
}, 60_000);
