import type { HandlerMap } from "./handler";

import { notFound } from "./handler";

/**
 * Sync jobs, the connection status that derives from them, and the connection timeline. mock/jobs.ts keeps the jobs
 * and their events, and ends a job that the test does not hold. mock/lifecycle.ts lets a test hold a job and end it.
 */
export const jobHandlers: HandlerMap = {
  syncConnection: ({ body, store }) =>
    store.connections.find(body.connectionId) ? store.jobs.startSync(body.connectionId) : notFound(),
  cancelJob: ({ body, store }) => store.jobs.cancelByUser(body.id) ?? notFound(),

  getConnectionStatuses: ({ body, store }) => {
    if (body.connectionIds.some((connectionId) => !store.connections.find(connectionId))) {
      return notFound();
    }
    return body.connectionIds.map((connectionId) => store.jobs.connectionStatus(connectionId));
  },
  /** The webapp builds the "Sync running" timeline row from the running job. */
  getConnectionSyncProgress: ({ body, store }) => {
    if (!store.connections.find(body.connectionId)) {
      return notFound();
    }
    const running = store.jobs.runningJob(body.connectionId);
    return {
      connectionId: body.connectionId,
      configType: "sync",
      streams: [],
      ...(running && { jobId: running.job.id, syncStartedAt: running.job.createdAt }),
    };
  },

  /** The mock returns all events in one page, so it answers a request for a later page with an empty list. */
  listConnectionEvents: ({ body, store }) => {
    if (!store.connections.find(body.connectionId)) {
      return notFound();
    }
    if (body.eventTypes || body.createdAtStart || body.createdAtEnd) {
      throw new Error("The mock does not support filters on the connection timeline");
    }
    return { events: body.pagination?.rowOffset ? [] : [...store.jobs.events(body.connectionId)] };
  },
  listConnectionEventsForJob: ({ body, store }) =>
    store.jobs.info(body.id) ? { events: [...store.jobs.eventsForJob(body.id)] } : notFound(),
  getConnectionEvent: ({ body, store }) => {
    const event = store.jobs.event(body.connectionEventId);
    return event && event.connectionId === body.connectionId ? { ...event, details: {} } : notFound();
  },

  getJobInfoWithoutLogs: ({ body, store }) => store.jobs.info(body.id) ?? notFound(),
  /** Jobs in the mock move no data, so every count is zero. */
  getAttemptCombinedStats: ({ body, store }) =>
    store.jobs.info(body.jobId)?.attempts[body.attemptNumber]
      ? { recordsEmitted: 0, bytesEmitted: 0, recordsCommitted: 0, bytesCommitted: 0 }
      : notFound(),
  /** The logs of an attempt are empty. */
  getAttemptForJob: ({ body, store }) => {
    const attempt = store.jobs.info(body.jobId)?.attempts[body.attemptNumber];
    return attempt ? { ...attempt, logType: "formatted", logs: { logLines: [] } } : notFound();
  },
};
