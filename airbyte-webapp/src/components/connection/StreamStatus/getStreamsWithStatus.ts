import { useMemo } from "react";

import {
  AirbyteStreamAndConfiguration,
  AirbyteStreamConfiguration,
  ConnectionScheduleData,
  ConnectionScheduleType,
  JobStatus,
  WebBackendConnectionRead,
  AttemptStatus,
  JobWithAttemptsRead,
  AttemptRead,
  JobRead,
} from "core/request/AirbyteClient";

export interface FakeStreamConfigWithStatus extends AirbyteStreamConfiguration {
  jobStatus?: JobStatus;
  lastSuccessfulSync?: number;
  latestAttemptStatus?: AttemptStatus;
  latestAttemptCreatedAt?: number;
  scheduleType?: ConnectionScheduleType;
  scheduleData?: ConnectionScheduleData;
  isSyncing: boolean;
}

export interface AirbyteStreamWithStatusAndConfiguration extends AirbyteStreamAndConfiguration {
  config?: FakeStreamConfigWithStatus;
}

function filterStreamsWithTypecheck(
  v: AirbyteStreamWithStatusAndConfiguration | null
): v is AirbyteStreamWithStatusAndConfiguration {
  return Boolean(v);
}

interface StreamStats {
  createdAt?: number;
  attemptStatus?: AttemptStatus;
  jobStatus?: JobStatus;
  lastSuccessfulSync?: number;
}

const getStreamKey = (name: string, namespace = "") => `${namespace}-${name}`;

const addStatToStream = (
  key: string,
  streamStats: Record<string, StreamStats>,
  job: JobRead,
  attempt?: AttemptRead
) => {
  streamStats[key] ??= {};
  if (attempt && !streamStats[key].attemptStatus) {
    streamStats[key].attemptStatus ??= attempt.status;

    if ((streamStats[key].createdAt ?? 0) < attempt.createdAt) {
      streamStats[key].createdAt = attempt.createdAt;
    }
    if (attempt.status === AttemptStatus.succeeded && (streamStats[key].lastSuccessfulSync ?? 0) < attempt.createdAt) {
      streamStats[key].lastSuccessfulSync = attempt.createdAt;
    }
  }

  streamStats[key].jobStatus ??= job.status;
  if ((streamStats[key].createdAt ?? 0) < job.createdAt) {
    streamStats[key].createdAt = job.createdAt;
  }
  if (job.status === JobStatus.succeeded && (streamStats[key].lastSuccessfulSync ?? 0) < job.createdAt) {
    streamStats[key].lastSuccessfulSync = job.createdAt;
  }
};

// Due to the constraints around v1 per-stream status work
// the connection status & data is being pushed into the streams
// to fake per-stream data. This should reduce the lift when
// per-stream status is available.
export const useStreamsWithStatus = (
  connection: WebBackendConnectionRead,
  jobs: JobWithAttemptsRead[]
): AirbyteStreamWithStatusAndConfiguration[] => {
  const streamStats: Record<string, StreamStats> = jobs.reduce((streamStats, jobAttempt) => {
    if (!jobAttempt || !jobAttempt.job) {
      return streamStats;
    }
    const { job, attempts } = jobAttempt;
    attempts?.forEach((attempt) => {
      attempt.streamStats
        // reverse so we get the latest
        ?.reverse()
        .forEach((streamStat) =>
          addStatToStream(getStreamKey(streamStat.streamName, streamStat.streamNamespace), streamStats, job, attempt)
        );
    });
    // This is populated on syncs
    job.enabledStreams?.forEach(({ name, namespace }) =>
      addStatToStream(getStreamKey(name, namespace), streamStats, job)
    );

    // This is populated for resets
    job.resetConfig?.streamsToReset?.forEach(({ name, namespace }) =>
      addStatToStream(getStreamKey(name, namespace), streamStats, job)
    );
    return streamStats;
  }, {} as Record<string, StreamStats>);

  const streamsWithStatus = useMemo(
    () =>
      connection.syncCatalog.streams
        .map<AirbyteStreamWithStatusAndConfiguration | null>(({ stream, config }) => {
          if (stream && config) {
            const { lastSuccessfulSync, createdAt, attemptStatus, jobStatus } =
              streamStats[getStreamKey(stream.name, stream.namespace)] ?? {};
            const fakeStream: AirbyteStreamWithStatusAndConfiguration = {
              stream,
              config: {
                ...config,
                jobStatus,
                lastSuccessfulSync,
                latestAttemptStatus: attemptStatus,
                latestAttemptCreatedAt: createdAt,
                scheduleType: connection.scheduleType,
                scheduleData: connection.scheduleData,
                isSyncing: attemptStatus === AttemptStatus.running || jobStatus === JobStatus.running,
              },
            };
            return fakeStream;
          }
          return null;
        })
        .filter(filterStreamsWithTypecheck),
    [connection.scheduleData, connection.scheduleType, connection.syncCatalog.streams, streamStats]
  );

  return streamsWithStatus;
};
