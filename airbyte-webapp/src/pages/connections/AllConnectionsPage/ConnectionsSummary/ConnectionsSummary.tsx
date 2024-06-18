import React from "react";
import { FormattedMessage } from "react-intl";

import { Text } from "components/ui/Text";

import { useConnectionList } from "core/api";
import { JobStatus, WebBackendConnectionListItem } from "core/api/types/AirbyteClient";

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

export const ConnectionsSummary: React.FC = () => {
  const connections = useConnectionList()?.connections ?? [];

  const connectionsSummary = connections.reduce<Record<SummaryKey, number>>(
    (acc, connection) => {
      let status: SummaryKey;

      if (isConnectionPaused(connection)) {
        status = "paused";
      } else if (isConnectionRunning(connection)) {
        status = "running";
      } else if (isConnectionFailed(connection)) {
        status = "failed";
      } else {
        status = "healthy";
      }

      acc[status] += 1;
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

  return <>{parts}</>;
};
