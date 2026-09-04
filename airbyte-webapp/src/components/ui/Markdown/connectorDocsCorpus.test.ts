import fs from "fs";
import path from "path";
import { Worker } from "worker_threads";

import { preprocessMarkdown } from "./Markdown";

jest.setTimeout(10 * 60 * 1000);

const fixturesRoot = path.resolve(__dirname, "__fixtures__/connector-docs");
const workerPath = path.resolve(__dirname, "compileMarkdownWorker.mjs");

type CompileResult = { id: number; ms: number } | { id: number; error: string };
interface Failure {
  file: string;
  reason: string;
}

const findMarkdownFiles = (directory: string): string[] => {
  return fs.readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const entryPath = path.join(directory, entry.name);
    return entry.isDirectory() ? findMarkdownFiles(entryPath) : entry.name.endsWith(".md") ? [entryPath] : [];
  });
};

const createWorker = () => new Worker(workerPath);

const compileWithTimeout = (worker: Worker, id: number, markdown: string): Promise<CompileResult | "timeout"> => {
  return new Promise((resolve) => {
    const timeout = setTimeout(() => {
      worker.off("message", onMessage);
      worker.off("error", onError);
      resolve("timeout");
    }, 5000);

    const onMessage = (result: CompileResult) => {
      if (result.id !== id) {
        return;
      }
      clearTimeout(timeout);
      worker.off("message", onMessage);
      worker.off("error", onError);
      resolve(result);
    };
    const onError = (error: Error) => {
      clearTimeout(timeout);
      worker.off("message", onMessage);
      worker.off("error", onError);
      resolve({ id, error: error.stack || String(error) });
    };

    worker.on("message", onMessage);
    worker.once("error", onError);
    worker.postMessage({ id, markdown });
  });
};

describe("connector documentation corpus renders without hanging", () => {
  it("compiles every connector document within the time budget", async () => {
    const files = ["sources", "destinations"]
      .flatMap((category) => findMarkdownFiles(path.join(fixturesRoot, category)))
      .sort();
    expect(files.length).toBeGreaterThan(0);

    const failures: Failure[] = [];
    let worker = createWorker();

    try {
      for (const [index, filePath] of files.entries()) {
        const file = path.relative(fixturesRoot, filePath).split(path.sep).join("/");
        const processed = preprocessMarkdown(fs.readFileSync(filePath, "utf8"));
        const result = await compileWithTimeout(worker, index, processed);

        if (result === "timeout") {
          failures.push({ file, reason: "timeout >5000ms" });
          await worker.terminate();
          worker = createWorker();
        } else if ("error" in result) {
          failures.push({ file, reason: result.error });
          await worker.terminate();
          worker = createWorker();
        }
      }
    } finally {
      await worker.terminate();
    }

    expect(failures).toEqual([]);
  });
});
