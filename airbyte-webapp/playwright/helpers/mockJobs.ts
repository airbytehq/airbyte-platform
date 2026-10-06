import type { JobRead } from "@src/core/api/types/AirbyteClient";

import { expect } from "@playwright/test";

export function createMockJobs({ onJobChanged }: { onJobChanged: (job: Readonly<JobRead>) => void }) {
  let entries: JobRead[] = [];

  function list() {
    return structuredClone(entries);
  }

  function latestForConnection(connectionId: string) {
    const job = entries.findLast((job) => job.configId === connectionId);
    return job ? structuredClone(job) : undefined;
  }

  function startSync(connectionId: string) {
    const job: JobRead = {
      id: entries.length + 1,
      configId: connectionId,
      configType: "sync",
      status: "running",
      createdAt: 1791028800,
      updatedAt: 1791028800,
    };
    entries = [...entries, job];
    onJobChanged(structuredClone(job));
    return structuredClone(job);
  }

  function cancel(jobId: number) {
    const current = entries.find((job) => job.id === jobId);
    expect(current, `Unknown job ID: ${jobId}`).toBeDefined();
    expect(current!.status).toBe("running");

    const job: JobRead = { ...current!, status: "cancelled" };
    entries = entries.map((entry) => (entry.id === jobId ? job : entry));
    onJobChanged(structuredClone(job));
    return structuredClone(job);
  }

  return { list, latestForConnection, startSync, cancel };
}

export type MockJobs = ReturnType<typeof createMockJobs>;
