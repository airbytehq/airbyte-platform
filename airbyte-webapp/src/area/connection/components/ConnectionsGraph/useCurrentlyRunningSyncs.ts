import dayjs from "dayjs";
import { v4 as uuid } from "uuid";

import { useGetCachedConnectionStatusesById } from "core/api";
import { ConnectionSyncStatus } from "core/api/types/AirbyteClient";

export interface RunningJobEvent {
  createdAt: string;
  eventType: "RUNNING_JOB";
  connectionId: string;
  eventId: string;
}

// Because the backend does not track running syncs in the timeline, we manually add them to the events list
// by querying the connection statuses and creating a SYNC_STARTED event for each running connection
export const useCurrentlyRunningSyncs = (connectionIds: string[]): RunningJobEvent[] => {
  const connectionStatuses = useGetCachedConnectionStatusesById(connectionIds);

  const statuses = Object.values(connectionStatuses);

  return statuses
    .filter((status): status is NonNullable<typeof status> => status !== undefined)
    .filter((status) => status.connectionSyncStatus === ConnectionSyncStatus.running)
    .map((status) => ({
      createdAt: status.lastSyncJobCreatedAt
        ? dayjs(status.lastSyncJobCreatedAt * 1000).toISOString()
        : dayjs().toISOString(),
      eventType: "RUNNING_JOB",
      connectionId: status.connectionId,
      eventId: uuid(),
    }));
};
