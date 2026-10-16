import type { Body, Op } from "./routes";

import { snapshot } from "./store";

export interface RecordedCall {
  op: Op;
  method: string;
  path: string;
  body: unknown;
  status: number;
}

/**
 * The requests the webapp sent, by operationId. Use it to assert what the webapp did:
 * `expect(airbyte.calls("deleteConnection")).toEqual([{ connectionId }])`.
 */
export interface Recorder {
  /** The request bodies the webapp sent for one operation, oldest first. Each call returns a new array. */
  <O extends Op>(op: O): Array<Body<O>>;
  /** Body of the latest call of one operation. */
  last<O extends Op>(op: O): Body<O> | undefined;
  count(op: Op): number;
  /** Every answered call, all operations, in order. Attached to the test report on failure. */
  log(): readonly RecordedCall[];
}

export function createRecorder() {
  const entries: RecordedCall[] = [];

  const bodies = <O extends Op>(op: O) =>
    entries.filter((entry) => entry.op === op).map((entry) => entry.body as Body<O>);

  const calls: Recorder = Object.assign(bodies, {
    last: <O extends Op>(op: O) => bodies(op).at(-1),
    count: (op: Op) => bodies(op).length,
    log: () => [...entries],
  });

  let waiters: Array<{ op: Op; resolve: () => void }> = [];

  function record(entry: RecordedCall) {
    entries.push(snapshot(entry));
    const answered = waiters.filter(({ op }) => op === entry.op);
    waiters = waiters.filter(({ op }) => op !== entry.op);
    answered.forEach(({ resolve }) => resolve());
  }

  /** Resolves when the webapp next sends a request for `op`. Used by lifecycle.ts, not by tests. */
  function nextCall(op: Op) {
    return new Promise<void>((resolve) => waiters.push({ op, resolve }));
  }

  return { calls, record, nextCall };
}
