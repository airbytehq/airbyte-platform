import { CellContext } from "@tanstack/react-table";
import classNames from "classnames";
import React, { Suspense } from "react";
import { FormattedMessage } from "react-intl";

import { useConnectionStatus } from "components/connection/ConnectionStatus/useConnectionStatus";
import { ConnectionStatusIndicator } from "components/connection/ConnectionStatusIndicator";
import { StreamWithStatus, sortStreamsByStatus } from "components/connection/StreamStatus/streamStatusUtils";
import { StreamStatusIndicator, StreamStatusType } from "components/connection/StreamStatusIndicator";
import { LoadingSpinner } from "components/ui/LoadingSpinner";
import { Tooltip } from "components/ui/Tooltip";

import { AirbyteStreamAndConfigurationWithEnforcedStream, useStreamsStatuses } from "area/connection/utils";

import styles from "./StreamStatusCell.module.scss";
import { ConnectionTableDataItem } from "../types";

const FILLING_STYLE_BY_STATUS: Readonly<Record<StreamStatusType, string>> = {
  [StreamStatusType.Failed]: styles["filling--failed"],
  [StreamStatusType.Synced]: styles["filling--upToDate"],
  [StreamStatusType.Paused]: styles["filling--paused"],
  [StreamStatusType.Incomplete]: styles["filling--incomplete"],
  [StreamStatusType.Pending]: styles["filling--pending"],
  [StreamStatusType.Queued]: styles["filling--queued"],
  [StreamStatusType.Syncing]: styles["filling--syncing"],
  [StreamStatusType.Clearing]: styles["filling--syncing"],
  [StreamStatusType.Refreshing]: styles["filling--syncing"],
  [StreamStatusType.QueuedForNextSync]: styles["filling--queued"],
  [StreamStatusType.RateLimited]: styles["filling--syncing"],
};

const StreamsBar: React.FC<{
  streams: Array<[string, StreamWithStatus[]]>;
  totalStreamCount: number;
}> = ({ streams, totalStreamCount }) => {
  return (
    <div className={styles.bar}>
      {streams.map(([status, streams]) => {
        const widthPercentage = Number(streams.length / totalStreamCount) * 100;
        const fillingModifier = FILLING_STYLE_BY_STATUS[status as StreamStatusType];

        return (
          <div
            style={{ width: `${widthPercentage}%` }}
            className={classNames(styles.filling, fillingModifier)}
            key={status}
          />
        );
      })}
    </div>
  );
};

const SyncingStreams: React.FC<{ streams: StreamWithStatus[] }> = ({ streams }) => {
  const syncingStreamsCount = streams.filter((stream) => stream.isRunning).length;

  if (syncingStreamsCount) {
    return (
      <div className={styles.syncContainer}>
        <LoadingSpinner className={styles.loadingSpinner} />
        <strong>{syncingStreamsCount}</strong>&nbsp;running
      </div>
    );
  }
  return null;
};

const StreamsPerStatus: React.FC<{
  streamStatuses: Map<string, StreamWithStatus>;
  enabledStreams: AirbyteStreamAndConfigurationWithEnforcedStream[];
}> = ({ streamStatuses, enabledStreams }) => {
  const sortedStreamsMap = sortStreamsByStatus(enabledStreams, streamStatuses);
  const filteredAndSortedStreams = Object.entries(sortedStreamsMap).filter(([, streams]) => !!streams.length) as Array<
    [StreamStatusType, StreamWithStatus[]]
  >;

  return (
    <>
      {filteredAndSortedStreams.map(([statusType, streams]) => (
        <div className={styles.tooltipContent} key={statusType}>
          <div className={styles.streamsDetail}>
            <StreamStatusIndicator status={statusType} />
            <strong>{streams.length}</strong> <FormattedMessage id={`connection.stream.status.${statusType}`} />
          </div>
          <SyncingStreams streams={streams} />
        </div>
      ))}
    </>
  );
};

const StreamStatusPopover: React.FC<{ connectionId: string }> = ({ connectionId }) => {
  const { streamStatuses, enabledStreams } = useStreamsStatuses(connectionId);
  const sortedStreamsMap = sortStreamsByStatus(enabledStreams, streamStatuses);
  const filteredAndSortedStreamsMap = Object.entries(sortedStreamsMap).filter(([, streams]) => !!streams.length);
  return (
    <div className={styles.tooltipContainer}>
      <StreamsBar streams={filteredAndSortedStreamsMap} totalStreamCount={enabledStreams.length} />
      <StreamsPerStatus streamStatuses={streamStatuses} enabledStreams={enabledStreams} />
    </div>
  );
};

export const StreamsStatusCell: React.FC<CellContext<ConnectionTableDataItem, unknown>> = ({ row }) => {
  const connectionId = row.original.connectionId;
  const { status } = useConnectionStatus(connectionId);

  return (
    <Tooltip theme="light" control={<ConnectionStatusIndicator status={status} />}>
      <Suspense fallback={null}>
        <StreamStatusPopover connectionId={connectionId} />
      </Suspense>
    </Tooltip>
  );
};
