import { createColumnHelper } from "@tanstack/react-table";
import classNames from "classnames";
import dayjs from "dayjs";
import React, { useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { useToggle } from "react-use";

import {
  AirbyteStreamWithStatusAndConfiguration,
  FakeStreamConfigWithStatus,
} from "components/connection/StreamStatus/getStreamsWithStatus";
import { useGetStreamStatus } from "components/connection/StreamStatus/streamStatusUtils";
import { StreamStatusIndicator } from "components/connection/StreamStatusIndicator";
import { TimeIcon } from "components/icons/TimeIcon";
import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Table } from "components/ui/Table";
import { Text } from "components/ui/Text";

import { ConnectionStatusCard } from "./ConnectionStatusCard";
import { StreamActionsMenu } from "./StreamActionsMenu";
import { StreamSearchFiltering } from "./StreamSearchFiltering";
import styles from "./StreamsList.module.scss";
import { useStreamsListContext } from "./StreamsListContext";

const LastSync: React.FC<{ config: FakeStreamConfigWithStatus | undefined; showRelativeTime: boolean }> = ({
  config,
  showRelativeTime,
}) => {
  const lastSyncDisplayText = useMemo(() => {
    if (config?.lastSuccessfulSync) {
      const lastSync = dayjs(config.lastSuccessfulSync * 1000);

      if (showRelativeTime) {
        return lastSync.fromNow();
      }
      return lastSync.format("MM.DD.YY HH:mm:ss");
    }
    return null;
  }, [config?.lastSuccessfulSync, showRelativeTime]);
  if (lastSyncDisplayText) {
    return (
      <Text size="xs" color="grey300">
        {lastSyncDisplayText}
      </Text>
    );
  }
  return null;
};

export const StreamsList = () => {
  const [showRelativeTime, setShowRelativeTime] = useToggle(true);

  const { streams, filteredStreams } = useStreamsListContext();

  const columnHelper = useMemo(() => createColumnHelper<AirbyteStreamWithStatusAndConfiguration>(), []);
  const getStreamStatus = useGetStreamStatus();

  const columns = useMemo(
    () => [
      columnHelper.accessor("config", {
        id: "statusIcon",
        header: () => <FormattedMessage id="connection.stream.status.table.status" />,
        cell: (props) => (
          <StreamStatusIndicator
            status={getStreamStatus(props.cell.getValue())}
            loading={props.cell.getValue()?.isSyncing || props.cell.getValue()?.isResetting}
          />
        ),
        meta: {
          thClassName: styles.statusHeader,
          tdClassName: styles.statusCell,
        },
      }),
      columnHelper.accessor("stream.name", {
        header: () => <FormattedMessage id="connection.stream.status.table.streamName" />,
        cell: (props) => <>{props.cell.getValue()}</>,
      }),
      columnHelper.accessor("config", {
        id: "lastSync",
        header: () => (
          <button onClick={setShowRelativeTime} className={styles.clickableHeader}>
            <FormattedMessage id={`connection.stream.status.table.lastRecord${showRelativeTime ? "" : "At"}`} />
            <TimeIcon />
          </button>
        ),
        cell: (props) => <LastSync config={props.cell.getValue()} showRelativeTime={showRelativeTime} />,
      }),
      columnHelper.accessor("config", {
        header: () => null,
        id: "actions",
        cell: (props) => <StreamActionsMenu stream={props.row.original.stream} />,
        meta: {
          thClassName: styles.actionsHeader,
        },
      }),
    ],
    [columnHelper, getStreamStatus, setShowRelativeTime, showRelativeTime]
  );

  return (
    <>
      <Box mb="md">
        <ConnectionStatusCard streamCount={streams.length} />
      </Box>
      <Card
        title={
          <FlexContainer justifyContent="space-between" alignItems="center">
            <FormattedMessage id="connection.stream.status.title" />
            <StreamSearchFiltering className={styles.search} />
          </FlexContainer>
        }
      >
        <FlexContainer direction="column" gap="sm" className={styles.body}>
          <div className={styles.tableContainer} data-survey="streamcentric">
            <Table
              columns={columns}
              data={filteredStreams}
              variant="inBlock"
              className={styles.table}
              getRowClassName={(data) =>
                classNames({ [styles.syncing]: data.config?.isSyncing || data.config?.isResetting })
              }
            />
          </div>
        </FlexContainer>
      </Card>
    </>
  );
};
