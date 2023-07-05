import { AttemptRead, FailureReason, FailureType, JobStatus, SynchronousJobRead } from "core/request/AirbyteClient";

import { JobWithAttempts } from "./types";

export const getFailureFromAttempt = (attempt: AttemptRead): FailureReason | undefined =>
  attempt.failureSummary?.failures[0];

export const isCancelledAttempt = (attempt: AttemptRead): boolean =>
  attempt.failureSummary?.failures.some(({ failureType }) => failureType === FailureType.manual_cancellation) ?? false;

export const didJobSucceed = (job: SynchronousJobRead | JobWithAttempts): boolean =>
  "succeeded" in job ? job.succeeded : getJobStatus(job) !== "failed";

export const getJobStatus: (job: SynchronousJobRead | JobWithAttempts) => JobStatus = (job) =>
  "succeeded" in job ? (job.succeeded ? JobStatus.succeeded : JobStatus.failed) : job.job.status;

export const getJobAttempts: (job: SynchronousJobRead | JobWithAttempts) => AttemptRead[] | undefined = (job) =>
  "attempts" in job ? job.attempts : undefined;

export const getJobId = (job: SynchronousJobRead | JobWithAttempts): string =>
  "id" in job ? job.id : String(job.job.id);
