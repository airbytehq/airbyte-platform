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
  JobConfigType,
} from "core/request/AirbyteClient";

export interface FakeStreamConfigWithStatus extends AirbyteStreamConfiguration {
  jobStatus?: JobStatus;
  lastSuccessfulSync?: number;
  latestAttemptStatus?: AttemptStatus;
  latestAttemptCreatedAt?: number;
  scheduleType?: ConnectionScheduleType;
  scheduleData?: ConnectionScheduleData;
  isSyncing: boolean;
  isResetting?: boolean;
  jobId?: number;
  attemptId?: number;
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
  jobConfigType?: JobConfigType;
  jobId?: number;
  attemptId?: number;
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
    streamStats[key].jobId ??= job.id;
    streamStats[key].attemptId ??= attempt.id;

    if ((streamStats[key].createdAt ?? 0) < attempt.createdAt) {
      streamStats[key].createdAt = attempt.createdAt;
    }
    if (attempt.status === AttemptStatus.succeeded && (streamStats[key].lastSuccessfulSync ?? 0) < attempt.createdAt) {
      streamStats[key].lastSuccessfulSync = attempt.createdAt;
    }
  }

  streamStats[key].jobConfigType ??= job.configType;
  streamStats[key].jobStatus ??= job.status;
  streamStats[key].jobId ??= job.id;
  streamStats[key].attemptId ??= 0;

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
  const streamStats: Record<string, StreamStats> = useMemo(
    () =>
      jobs.reduce((streamStats, jobAttempt) => {
        if (!jobAttempt || !jobAttempt.job) {
          return streamStats;
        }
        const { job, attempts } = jobAttempt;
        [...(attempts || [])] // avoid mutating the platform response
          .sort((a, b) => {
            // ensure the most recent attempt (highest id) is processed first
            if (a.id < b.id) {
              return 1;
            } else if (a.id > b.id) {
              return -1;
            }
            return 0;
          })
          .forEach((attempt) => {
            attempt.streamStats?.forEach((streamStat) =>
              addStatToStream(
                getStreamKey(streamStat.streamName, streamStat.streamNamespace),
                streamStats,
                job,
                attempt
              )
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
      }, {} as Record<string, StreamStats>),
    [jobs]
  );

  const streamsWithStatus = useMemo(
    () =>
      connection.syncCatalog.streams
        .map<AirbyteStreamWithStatusAndConfiguration | null>(({ stream, config }) => {
          if (stream && config) {
            const { lastSuccessfulSync, createdAt, attemptStatus, jobStatus, jobConfigType, jobId, attemptId } =
              streamStats[getStreamKey(stream.name, stream.namespace)] ?? {};
            const isSyncing =
              jobConfigType === JobConfigType.sync &&
              (attemptStatus === AttemptStatus.running || jobStatus === JobStatus.running);
            const isResetting = jobStatus === JobStatus.running && jobConfigType === JobConfigType.reset_connection;

            const fakeStream: AirbyteStreamWithStatusAndConfiguration = {
              stream,
              config: {
                ...config,
                jobStatus,
                lastSuccessfulSync,
                jobId,
                attemptId,
                latestAttemptStatus: attemptStatus,
                latestAttemptCreatedAt: createdAt,
                scheduleType: connection.scheduleType,
                scheduleData: connection.scheduleData,
                isSyncing,
                isResetting,
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
