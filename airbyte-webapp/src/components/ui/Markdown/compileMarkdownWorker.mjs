import { parentPort } from "worker_threads";
import { compiler } from "markdown-to-jsx";

parentPort.on("message", ({ id, markdown }) => {
  const start = performance.now();
  try {
    compiler(markdown);
    parentPort.postMessage({ id, ms: performance.now() - start });
  } catch (e) {
    parentPort.postMessage({ id, error: String((e && e.stack) || e) });
  }
});
