import type {
  ConnectionEvent,
  ConnectionStatusRead,
  ConnectionSyncStatus,
  FailureReason,
  JobInfoRead,
  JobStatus,
  UserReadInConnectionEvent,
} from "@src/core/api/types/AirbyteClient";

import { events } from "./builders/events";
import { ids } from "./ids";
import { snapshot } from "./store";

/** Why a sync failed. The mock sets the timestamp to the end time of the job. */
export type SyncFailure = Omit<FailureReason, "timestamp">;

/** The result of a sync that ends on its own. Set it in a scenario as `outcomes.sync`. */
export type SyncOutcome = { status: "succeeded" } | { status: "failed"; failureReason?: SyncFailure };

/** The connection status for the status of its latest job. A cancelled sync leaves the connection incomplete. */
const syncStatusOfJob: Record<JobStatus, ConnectionSyncStatus> = {
  pending: "pending",
  queued: "queued",
  running: "running",
  incomplete: "incomplete",
  failed: "failed",
  succeeded: "synced",
  cancelled: "incomplete",
};

/** How a sync job ended. */
export type SyncEnd = SyncOutcome | { status: "cancelled"; user?: UserReadInConnectionEvent };

interface JobsOptions {
  /** The current browser time in epoch seconds (clock.ts). */
  nowSeconds: () => number;
  /**
   * The user who starts and cancels syncs in the webapp. The mock reads it when it writes an event, because instance
   * setup can change the email of the user during a test.
   */
  user: () => UserReadInConnectionEvent;
  outcome?: SyncOutcome;
}

/**
 * Sync jobs and their timeline events. The server writes an event when a job starts and when it ends, so the
 * events always match the job status.
 *
 * A job the webapp starts has one running attempt. It ends the first time the webapp reads it, with the scenario
 * outcome, unless the test holds it (lifecycle.ts). Each method that returns a job, a status or an event first ends
 * every running job that is not held.
 */
export function createJobs({ nowSeconds, user, outcome = { status: "succeeded" } }: JobsOptions) {
  const jobs: JobInfoRead[] = [];
  // The IDs of the jobs that the test holds. They are kept apart, so the stored jobs are plain API objects.
  const heldJobIds = new Set<number>();
  // The job ID of each event is kept next to the event, because the API types the event summary as any JSON.
  const timeline: Array<{ jobId: number; event: ConnectionEvent }> = [];
  let holdJobs = false;

  function addEvent(jobId: number, build: (id: string) => ConnectionEvent) {
    timeline.push({ jobId, event: build(ids.event(timeline.length + 1)) });
  }

  function startSync(connectionId: string) {
    const now = nowSeconds();
    const jobInfo: JobInfoRead = {
      job: {
        id: jobs.length + 1,
        configType: "sync",
        configId: connectionId,
        status: "running",
        createdAt: now,
        updatedAt: now,
      },
      attempts: [{ attempt: { id: 0, status: "running", createdAt: now, updatedAt: now } }],
    };
    jobs.push(jobInfo);
    if (holdJobs) {
      heldJobIds.add(jobInfo.job.id);
    }
    addEvent(jobInfo.job.id, (id) => events.syncStarted({ id, jobInfo, user: user() }));
    return snapshot(jobInfo);
  }

  function end(jobInfo: JobInfoRead, result: SyncEnd) {
    const now = nowSeconds();
    const attempt = jobInfo.attempts.at(-1)!.attempt;
    jobInfo.job.status = result.status;
    jobInfo.job.updatedAt = now;
    // The API has no "cancelled" attempt status. The server marks the attempt of a cancelled job as failed.
    attempt.status = result.status === "succeeded" ? "succeeded" : "failed";
    attempt.updatedAt = now;
    attempt.endedAt = now;

    if (result.status === "succeeded") {
      addEvent(jobInfo.job.id, (id) => events.syncSucceeded({ id, jobInfo }));
    } else if (result.status === "failed") {
      const failureReason = result.failureReason && { ...result.failureReason, timestamp: now * 1000 };
      // The server keeps the reason in the failure summary of the attempt. The connection status reads it from there.
      attempt.failureSummary = failureReason && { failures: [failureReason] };
      addEvent(jobInfo.job.id, (id) => events.syncFailed({ id, jobInfo, failureReason }));
    } else {
      addEvent(jobInfo.job.id, (id) => events.syncCancelled({ id, jobInfo, user: result.user }));
    }
  }

  /** Ends every running job that is not held, with the scenario outcome. */
  function endUnheldJobs() {
    jobs
      .filter(({ job }) => job.status === "running" && !heldJobIds.has(job.id))
      .forEach((jobInfo) => end(jobInfo, outcome));
  }

  function endUnheldJobsThenRead<T>(get: () => T) {
    endUnheldJobs();
    return snapshot(get());
  }

  function latestFor(connectionId: string) {
    return jobs.findLast(({ job }) => job.configId === connectionId);
  }

  /** The status the connection pages poll. It derives from the latest job of the connection. */
  function connectionStatus(connectionId: string): ConnectionStatusRead {
    const latest = latestFor(connectionId);
    if (!latest) {
      return { connectionId, connectionSyncStatus: "pending" };
    }

    const lastSucceeded = jobs.findLast(({ job }) => job.configId === connectionId && job.status === "succeeded");
    // As on the server (ConnectionsHandler.getConnectionStatuses), the failure reason comes from the last job that
    // succeeded or failed. A cancelled job after it does not hide the failure.
    const lastEnded = jobs.findLast(
      ({ job }) => job.configId === connectionId && (job.status === "succeeded" || job.status === "failed")
    );
    const failureReason =
      lastEnded?.job.status === "failed" ? lastEnded.attempts.at(-1)!.attempt.failureSummary?.failures[0] : undefined;
    return {
      connectionId,
      connectionSyncStatus: syncStatusOfJob[latest.job.status],
      activeJob: latest.job.status === "running" ? latest.job : undefined,
      lastSuccessfulSync: lastSucceeded?.job.updatedAt,
      lastSyncJobId: latest.job.id,
      lastSyncJobCreatedAt: latest.job.createdAt,
      lastSyncAttemptNumber: latest.attempts.length - 1,
      failureReason,
    };
  }

  /** Ends the latest held job that is still running. Throws when there is none. */
  function endHeld(result: SyncEnd) {
    const jobInfo = jobs.findLast(({ job }) => heldJobIds.has(job.id) && job.status === "running");
    if (!jobInfo) {
      throw new Error("No held job is running. Call airbyte.jobs.hold() before the webapp starts the sync.");
    }
    end(jobInfo, result);
    return snapshot(jobInfo);
  }

  return {
    startSync,
    /** Cancels a running job for the user, as the cancel button does. A job that has ended stays as it is. */
    cancelByUser: (jobId: number) => {
      const jobInfo = jobs.find(({ job }) => job.id === jobId);
      if (jobInfo?.job.status === "running") {
        end(jobInfo, { status: "cancelled", user: user() });
      }
      return jobInfo && snapshot(jobInfo);
    },
    info: (jobId: number) => endUnheldJobsThenRead(() => jobs.find(({ job }) => job.id === jobId)),
    connectionStatus: (connectionId: string) => endUnheldJobsThenRead(() => connectionStatus(connectionId)),
    /** The latest sync job of a connection, or undefined when the webapp started none. */
    latestJob: (connectionId: string) => endUnheldJobsThenRead(() => latestFor(connectionId)),
    runningJob: (connectionId: string) =>
      endUnheldJobsThenRead(() => jobs.find(({ job }) => job.configId === connectionId && job.status === "running")),
    /** Timeline events of one connection, newest first, as the server sorts them. */
    events: (connectionId: string) =>
      endUnheldJobsThenRead(() =>
        timeline
          .filter(({ event }) => event.connectionId === connectionId)
          .map(({ event }) => event)
          .reverse()
      ),
    /** Timeline events of one job, newest first. */
    eventsForJob: (jobId: number) =>
      endUnheldJobsThenRead(() =>
        timeline
          .filter((entry) => entry.jobId === jobId)
          .map(({ event }) => event)
          .reverse()
      ),
    event: (eventId: string) => endUnheldJobsThenRead(() => timeline.find(({ event }) => event.id === eventId)?.event),
    hold: () => {
      holdJobs = true;
    },
    all: () => jobs.map(snapshot),
    endHeld,
  };
}

export type Jobs = ReturnType<typeof createJobs>;
