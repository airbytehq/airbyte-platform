import type {
  ConnectionEvent,
  FailureReason,
  JobInfoRead,
  UserReadInConnectionEvent,
} from "@src/core/api/types/AirbyteClient";

/**
 * Timeline events of sync jobs, as the server writes them when a job starts or ends.
 *
 * The API types the `summary` of an event as any JSON object, so TypeScript cannot check it. The webapp checks
 * each event against a zod schema (eventTypeToSchemaMap in src/pages/connections/ConnectionTimelinePage/types.ts)
 * and does not show an event that fails it. tests/mock-backend/events.spec.ts runs every builder here through
 * those schemas.
 */
interface JobEventInput {
  id: string;
  /** The job with its attempts. */
  jobInfo: JobInfoRead;
  /** The user who started or cancelled the job. Jobs that the scheduler starts or ends have no user. */
  user?: UserReadInConnectionEvent;
}

/** Summary fields of every job event. */
function jobSummary({ job, attempts }: JobInfoRead) {
  return { jobId: job.id, startTimeEpochSeconds: job.createdAt, attemptsCount: attempts.length };
}

/** An event for a job that has ended. The event time is the end time of the job. */
function endedJobEvent(eventType: ConnectionEvent["eventType"], { id, jobInfo, user }: JobEventInput) {
  return {
    id,
    connectionId: jobInfo.job.configId,
    eventType,
    createdAt: jobInfo.job.updatedAt,
    user,
    summary: { ...jobSummary(jobInfo), endTimeEpochSeconds: jobInfo.job.updatedAt },
  } satisfies ConnectionEvent;
}

export const events = {
  syncStarted: ({ id, jobInfo, user }: JobEventInput): ConnectionEvent => ({
    id,
    connectionId: jobInfo.job.configId,
    eventType: "SYNC_STARTED",
    createdAt: jobInfo.job.createdAt,
    user,
    summary: jobSummary(jobInfo),
  }),
  syncSucceeded: (input: JobEventInput): ConnectionEvent => endedJobEvent("SYNC_SUCCEEDED", input),
  syncCancelled: (input: JobEventInput): ConnectionEvent => endedJobEvent("SYNC_CANCELLED", input),
  syncFailed: (input: JobEventInput & { failureReason?: FailureReason }): ConnectionEvent => {
    const event = endedJobEvent("SYNC_FAILED", input);
    return { ...event, summary: { ...event.summary, failureReason: input.failureReason } };
  },
};
