import React from "react";
import { FormattedMessage } from "react-intl";

import { LoadingSkeleton } from "components/ui/LoadingSkeleton";
import { Text } from "components/ui/Text";

import { useGetConnectionStatusesCounts } from "core/api";
import {
  ConnectionStatusRead,
  ConnectionSyncStatus,
  JobStatus,
  WebBackendConnectionListItem,
} from "core/api/types/AirbyteClient";

import styles from "./ConnectionsSummary.module.scss";

export type SummaryKey = "healthy" | "failed" | "paused" | "running" | "notSynced";

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

export const ConnectionsSummary: React.FC = () => {
  const { isLoading, data: statuses } = useGetConnectionStatusesCounts();

  if (isLoading || !statuses) {
    return <LoadingSkeleton className={styles.fixedHeight} />;
  }

  const statusItems: Array<{
    key: SummaryKey;
    count: number;
    color: React.ComponentProps<typeof Text>["color"];
    labelId: string;
  }> = [
    {
      key: "running",
      count: statuses.running,
      color: "blue",
      labelId: "tables.connections.filters.status.running",
    } as const,
    {
      key: "healthy",
      count: statuses.healthy,
      color: "green600",
      labelId: "tables.connections.filters.status.healthy",
    } as const,
    {
      key: "failed",
      count: statuses.failed,
      color: "red",
      labelId: "tables.connections.filters.status.failed",
    } as const,
    {
      key: "paused",
      count: statuses.paused,
      color: "grey",
      labelId: "tables.connections.filters.status.paused",
    } as const,
    {
      key: "notSynced",
      count: statuses.notSynced,
      color: "grey300",
      labelId: "tables.connections.filters.status.notSynced",
    } as const,
  ].filter((item) => item.count > 0);

  return (
    <div className={styles.fixedHeight}>
      {statusItems.map((item, index) => (
        <React.Fragment key={item.key}>
          <Text as="span" size="lg" color={item.color} className={styles.lowercase}>
            {item.count} <FormattedMessage id={item.labelId} />
          </Text>
          {index < statusItems.length - 1 && <SeparatorDot />}
        </React.Fragment>
      ))}
    </div>
  );
};

const SeparatorDot = () => (
  <Text as="span" size="lg" bold color="grey">
    &nbsp;&middot;&nbsp;
  </Text>
);
