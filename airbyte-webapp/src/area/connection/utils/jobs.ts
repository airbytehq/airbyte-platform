import { AttemptRead, JobStatus, SynchronousJobRead } from "core/api/types/AirbyteClient";

import { JobWithAttempts } from "../types/jobs";

export const didJobSucceed = (job: SynchronousJobRead | JobWithAttempts): boolean =>
  "succeeded" in job ? job.succeeded : getJobStatus(job) !== "failed";

export const getJobStatus: (job: SynchronousJobRead | JobWithAttempts) => JobStatus = (job) =>
  "succeeded" in job ? (job.succeeded ? JobStatus.succeeded : JobStatus.failed) : job.job.status;

export const getJobAttempts: (job: SynchronousJobRead | JobWithAttempts) => AttemptRead[] | undefined = (job) =>
  "attempts" in job ? job.attempts : undefined;

export const getJobCreatedAt = (job: SynchronousJobRead | JobWithAttempts) =>
  (job as SynchronousJobRead).createdAt ?? (job as JobWithAttempts).job.createdAt;

export const isJobPartialSuccess = (attempts?: AttemptRead[]) => {
  if (!attempts) {
    return false;
  }
  if (attempts.length > 0 && attempts[attempts.length - 1].status === JobStatus.failed) {
    return attempts.some((attempt) => attempt.failureSummary && attempt.failureSummary.partialSuccess);
  }
  return false;
};
