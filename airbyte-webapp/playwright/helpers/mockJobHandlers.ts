import type { ApiHandler } from "./mockApiHandlers";
import type { MockJobs } from "./mockJobs";
import type { JobIdRequestBody, JobInfoRead } from "@src/core/api/types/AirbyteClient";

import { handleRequest } from "./mockApiHandlers";

export function createJobHandlers(jobs: Pick<MockJobs, "cancel">): Record<string, ApiHandler> {
  function cancelJob(body: JobIdRequestBody) {
    const job = jobs.cancel(body.id);
    return { json: { job, attempts: [] } satisfies JobInfoRead };
  }

  return { "POST /api/v1/jobs/cancel": handleRequest(cancelJob) };
}
