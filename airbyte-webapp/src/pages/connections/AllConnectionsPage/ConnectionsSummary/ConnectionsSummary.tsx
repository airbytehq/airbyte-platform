import React from "react";
import { FormattedMessage } from "react-intl";

import { LoadingSkeleton } from "components/ui/LoadingSkeleton";
import { Text } from "components/ui/Text";

import {
  ConnectionStatusRead,
  ConnectionSyncStatus,
  JobStatus,
  WebBackendConnectionListItem,
} from "core/api/types/AirbyteClient";
import { useExperiment } from "hooks/services/Experiment";

import styles from "./ConnectionsSummary.module.scss";

export type SummaryKey = "healthy" | "failed" | "paused" | "running";

export const connectionStatColors: Record<SummaryKey, React.ComponentPropsWithoutRef<typeof Text>["color"]> = {
  healthy: "green600",
  failed: "red",
  paused: "grey",
  running: "blue",
};

export const isConnectionEnabled = (
  connection: WebBackendConnectionListItem
): connection is WebBackendConnectionListItem & { status: "active" } => connection.status === "active";

export const isConnectionPaused = (
  connection: WebBackendConnectionListItem
): connection is WebBackendConnectionListItem & { status: "inactive" } => connection.status === "inactive";

export const isConnectionRunning = (
  connection: WebBackendConnectionListItem
): connection is WebBackendConnectionListItem & { isSyncing: true } => connection.isSyncing;

export const isConnectionFailed = (
  connection: WebBackendConnectionListItem
): connection is WebBackendConnectionListItem & { latestSyncJobStatus: "failed" } =>
  connection.latestSyncJobStatus === JobStatus.failed ||
  connection.latestSyncJobStatus === JobStatus.cancelled ||
  connection.latestSyncJobStatus === JobStatus.incomplete;

export const isStatusEnabled = (
  connectionStatus?: ConnectionStatusRead
): connectionStatus is ConnectionStatusRead & {
  status: Omit<ConnectionSyncStatus, typeof ConnectionSyncStatus.paused>;
} => connectionStatus?.connectionSyncStatus !== ConnectionSyncStatus.paused;

export const isStatusPaused = (
  connectionStatus?: ConnectionStatusRead
): connectionStatus is ConnectionStatusRead & { status: typeof ConnectionSyncStatus.paused } =>
  connectionStatus?.connectionSyncStatus === ConnectionSyncStatus.paused;

export const isStatusRunning = (
  connectionStatus?: ConnectionStatusRead
): connectionStatus is ConnectionStatusRead & { status: typeof ConnectionSyncStatus.running } =>
  connectionStatus?.connectionSyncStatus === ConnectionSyncStatus.running;

export const isStatusFailed = (
  connectionStatus?: ConnectionStatusRead
): connectionStatus is ConnectionStatusRead & {
  status: typeof ConnectionSyncStatus.failed | typeof ConnectionSyncStatus.incomplete;
} =>
  connectionStatus?.connectionSyncStatus === ConnectionSyncStatus.failed ||
  connectionStatus?.connectionSyncStatus === ConnectionSyncStatus.incomplete;

const getSummaryFromStatuses = (statuses: ConnectionStatusRead[]) =>
  statuses.reduce<Record<SummaryKey, number>>(
    (acc, status) => {
      let statusSummary: SummaryKey;

      if (isStatusPaused(status)) {
        statusSummary = "paused";
      } else if (isStatusRunning(status)) {
        statusSummary = "running";
      } else if (isStatusFailed(status)) {
        statusSummary = "failed";
      } else {
        statusSummary = "healthy";
      }

      acc[statusSummary] += 1;
      return acc;
    },
    {
      // order here governs render order
      running: 0,
      healthy: 0,
      failed: 0,
      paused: 0,
    }
  );

const getSummaryFromConnections = (connections: WebBackendConnectionListItem[]) =>
  connections.reduce<Record<SummaryKey, number>>(
    (acc, connection) => {
      let statusSummary: SummaryKey;

      if (isConnectionPaused(connection)) {
        statusSummary = "paused";
      } else if (isConnectionRunning(connection)) {
        statusSummary = "running";
      } else if (isConnectionFailed(connection)) {
        statusSummary = "failed";
      } else {
        statusSummary = "healthy";
      }

      acc[statusSummary] += 1;
      return acc;
    },
    {
      // order here governs render order
      running: 0,
      healthy: 0,
      failed: 0,
      paused: 0,
    }
  );

export const ConnectionsSummary: React.FC<{
  connections: WebBackendConnectionListItem[];
  statuses?: ConnectionStatusRead[];
}> = ({ connections, statuses = [] }) => {
  const isAllConnectionsStatusEnabled = useExperiment("connections.connectionsStatusesEnabled");

  const connectionsSummary = isAllConnectionsStatusEnabled
    ? getSummaryFromStatuses(statuses)
    : getSummaryFromConnections(connections);

  const areStatusesLoading =
    connections.length > 0 && // are there connections
    Object.values(connectionsSummary).reduce((count, acc) => acc + count, 0) === 0; // but no summary counts;

  if (areStatusesLoading) {
    return <LoadingSkeleton className={styles.fixedHeight} />;
  }

  const keys = Object.keys(connectionsSummary) as SummaryKey[];
  const parts: React.ReactNode[] = [];
  const connectionsCount = keys.reduce((total, value) => total + connectionsSummary[value], 0);
  let consumedConnections = 0;

  for (const key of keys) {
    const value = connectionsSummary[key];
    if (value) {
      consumedConnections += value;
      parts.push(
        <Text key={key} as="span" size="lg" color={connectionStatColors[key]} className={styles.lowercase}>
          {value} <FormattedMessage id={`tables.connections.filters.status.${key}`} />
        </Text>,
        consumedConnections < connectionsCount && (
          <Text key={`${key}-middot`} as="span" size="lg" bold color="grey">
            &nbsp;&middot;&nbsp;
          </Text>
        )
      );
    }
  }

  return <div className={styles.fixedHeight}>{parts}</div>;
};
