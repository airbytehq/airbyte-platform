import { useMemo } from "react";

import {
  AirbyteStreamAndConfiguration,
  AirbyteStreamConfiguration,
  ConnectionScheduleData,
  ConnectionScheduleType,
  ConnectionStatus,
  JobStatus,
  JobWithAttemptsRead,
  WebBackendConnectionRead,
} from "core/request/AirbyteClient";

export interface FakeStreamConfigWithStatus extends AirbyteStreamConfiguration {
  status: ConnectionStatus;
  latestSyncJobStatus?: JobStatus;
  latestSyncJobCreatedAt?: number;
  scheduleType?: ConnectionScheduleType;
  scheduleData?: ConnectionScheduleData;
  isSyncing: boolean;
  lastSuccessfulSync?: number;
}

export interface AirbyteStreamWithStatusAndConfiguration extends AirbyteStreamAndConfiguration {
  config?: FakeStreamConfigWithStatus;
}

function filterStreamsWithTypecheck(
  v: AirbyteStreamWithStatusAndConfiguration | null
): v is AirbyteStreamWithStatusAndConfiguration {
  return Boolean(v);
}

// Due to the constraints around v1 per-stream status work
// the connection status & data is being pushed into the streams
// to fake per-stream data. This should reduce the lift when
// per-stream status is available.
export const useStreamsWithStatus = (
  connection: WebBackendConnectionRead,
  jobs: JobWithAttemptsRead[]
): AirbyteStreamWithStatusAndConfiguration[] => {
  const lastSuccessfulSync = useMemo(
    () =>
      jobs
        .sort((a, b) => (a.job?.startedAt ?? 0) - (b.job?.startedAt ?? 0))
        .filter((job) => job.job?.status === JobStatus.succeeded)?.[0]?.job?.createdAt,
    [jobs]
  );

  const streamsWithStatus = useMemo(
    () =>
      connection.syncCatalog.streams
        .map<AirbyteStreamWithStatusAndConfiguration | null>(({ stream, config }) => {
          if (stream && config) {
            const fakeStream: AirbyteStreamWithStatusAndConfiguration = {
              stream,
              config: {
                ...config,
                status: connection.status,
                latestSyncJobStatus: connection.latestSyncJobStatus,
                latestSyncJobCreatedAt: connection.latestSyncJobCreatedAt,
                scheduleType: connection.scheduleType,
                scheduleData: connection.scheduleData,
                isSyncing: connection.isSyncing,
                lastSuccessfulSync,
              },
            };
            return fakeStream;
          }
          return null;
        })
        .filter(filterStreamsWithTypecheck),
    [
      connection.isSyncing,
      connection.latestSyncJobCreatedAt,
      connection.latestSyncJobStatus,
      connection.scheduleData,
      connection.scheduleType,
      connection.status,
      connection.syncCatalog.streams,
      lastSuccessfulSync,
    ]
  );

  return streamsWithStatus;
};
