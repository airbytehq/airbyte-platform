import { CellContext } from "@tanstack/react-table";
import classNames from "classnames";
import React, { Suspense } from "react";
import { FormattedMessage } from "react-intl";

import { useConnectionStatus } from "components/connection/ConnectionStatus/useConnectionStatus";
import {
  ConnectionStatusIndicator,
  ConnectionStatusIndicatorStatus,
} from "components/connection/ConnectionStatusIndicator";
import { StreamWithStatus, sortStreams } from "components/connection/StreamStatus/streamStatusUtils";
import { StreamStatusIndicator, StreamStatusLoadingSpinner } from "components/connection/StreamStatusIndicator";
import { Tooltip } from "components/ui/Tooltip";

import { AirbyteStreamAndConfigurationWithEnforcedStream, useStreamsStatuses } from "area/connection/utils";

import styles from "./StreamStatusCell.module.scss";
import { ConnectionTableDataItem } from "../types";

const FILLING_STYLE_BY_STATUS: Readonly<Record<ConnectionStatusIndicatorStatus, string>> = {
  [ConnectionStatusIndicatorStatus.ActionRequired]: styles["filling--actionRequired"],
  [ConnectionStatusIndicatorStatus.OnTime]: styles["filling--upToDate"],
  [ConnectionStatusIndicatorStatus.OnTrack]: styles["filling--upToDate"],
  [ConnectionStatusIndicatorStatus.Disabled]: styles["filling--disabled"],
  [ConnectionStatusIndicatorStatus.Error]: styles["filling--error"],
  [ConnectionStatusIndicatorStatus.Late]: styles["filling--late"],
  [ConnectionStatusIndicatorStatus.Pending]: styles["filling--pending"],
};

const StreamsBar: React.FC<{
  streams: Array<[string, StreamWithStatus[]]>;
  totalStreamCount: number;
}> = ({ streams, totalStreamCount }) => {
  return (
    <div className={styles.bar}>
      {streams.map(([status, streams]) => {
        const widthPercentage = Number(streams.length / totalStreamCount) * 100;
        const fillingModifier = FILLING_STYLE_BY_STATUS[status as ConnectionStatusIndicatorStatus];

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
        <StreamStatusLoadingSpinner className={styles.loadingSpinner} />
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
  const sortedStreamsMap = sortStreams(enabledStreams, streamStatuses);
  const filteredAndSortedStreams = Object.entries(sortedStreamsMap).filter(([, streams]) => !!streams.length) as Array<
    [ConnectionStatusIndicatorStatus, StreamWithStatus[]]
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
  const sortedStreamsMap = sortStreams(enabledStreams, streamStatuses);
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
  const { status, isRunning } = useConnectionStatus(connectionId);

  return (
    <Tooltip theme="light" control={<ConnectionStatusIndicator status={status} loading={isRunning} />}>
      <Suspense fallback={null}>
        <StreamStatusPopover connectionId={connectionId} />
      </Suspense>
    </Tooltip>
  );
};
