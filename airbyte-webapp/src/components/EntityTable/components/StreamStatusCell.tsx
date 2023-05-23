import { CellContext } from "@tanstack/react-table";
import classNames from "classnames";
import { FormattedMessage } from "react-intl";

import {
  AirbyteStreamWithStatusAndConfiguration,
  useStreamsWithStatus,
} from "components/connection/StreamStatus/getStreamsWithStatus";
import {
  filterEmptyStreamStatuses,
  useSortStreams,
  StreamStatusType,
} from "components/connection/StreamStatus/streamStatusUtils";
import { StreamStatusIndicator, StreamStatusLoadingSpinner } from "components/connection/StreamStatusIndicator";
import { Tooltip } from "components/ui/Tooltip";

import { useGetConnection } from "hooks/services/useConnectionHook";

import styles from "./StreamStatusCell.module.scss";
import { ConnectionTableDataItem } from "../types";

const FILLING_STYLE_BY_STATUS: Readonly<Record<StreamStatusType, string>> = {
  [StreamStatusType.ActionRequired]: styles["filling--actionRequired"],
  [StreamStatusType.UpToDate]: styles["filling--upToDate"],
  [StreamStatusType.Disabled]: styles["filling--disabled"],
  [StreamStatusType.Error]: styles["filling--error"],
  [StreamStatusType.Late]: styles["filling--late"],
  [StreamStatusType.Pending]: styles["filling--pending"],
};

const StreamsBar: React.FC<{ streams: AirbyteStreamWithStatusAndConfiguration[] }> = ({ streams }) => {
  const filteredAndSortedStreams = filterEmptyStreamStatuses(useSortStreams(streams));
  return (
    <div className={styles.bar}>
      {filteredAndSortedStreams.map(([statusType, streamsByStatus]) => {
        const widthPercentage = Number(streamsByStatus.length / streams.length) * 100;
        const fillingModifier = FILLING_STYLE_BY_STATUS[statusType as unknown as StreamStatusType];

        return (
          <div
            style={{ width: `${widthPercentage}%` }}
            className={classNames(styles.filling, fillingModifier)}
            key={statusType}
          />
        );
      })}
    </div>
  );
};

const SyncingStreams: React.FC<{ streams: AirbyteStreamWithStatusAndConfiguration[] }> = ({ streams }) => {
  const syncingStreamsCount = streams.filter((stream) => stream.config?.isSyncing).length;
  if (syncingStreamsCount) {
    return (
      <div className={styles.syncContainer}>
        {syncingStreamsCount} <StreamStatusLoadingSpinner />
      </div>
    );
  }
  return null;
};

const StreamsPerStatus: React.FC<{ streams: AirbyteStreamWithStatusAndConfiguration[] }> = ({ streams }) => {
  const filteredAndSortedStreams = filterEmptyStreamStatuses(useSortStreams(streams));
  return (
    <>
      {filteredAndSortedStreams.map(([statusType, streams]) => (
        <div className={styles.tooltipContent} key={statusType}>
          <div className={styles.streamsDetail}>
            <StreamStatusIndicator status={statusType as StreamStatusType} /> <b>{streams.length}</b>{" "}
            <FormattedMessage id={`connection.stream.status.${statusType}`} />
          </div>
          <SyncingStreams streams={streams} />
        </div>
      ))}
    </>
  );
};

const StreamStatusPopover = ({ streams }: { streams: AirbyteStreamWithStatusAndConfiguration[] }) => {
  return (
    <div className={styles.tooltipContainer}>
      <StreamsBar streams={streams} />
      <StreamsPerStatus streams={streams} />
    </div>
  );
};

export const StreamsStatusCell: React.FC<CellContext<ConnectionTableDataItem, unknown>> = ({ row }) => {
  const connection = useGetConnection(row.original.connectionId);
  const fakeStreamsWithStatus = useStreamsWithStatus(connection, []);

  return (
    <Tooltip theme="light" control={<StreamsBar streams={fakeStreamsWithStatus} />}>
      <StreamStatusPopover streams={fakeStreamsWithStatus} />
    </Tooltip>
  );
};
