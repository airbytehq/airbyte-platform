import { createColumnHelper } from "@tanstack/react-table";
import classNames from "classnames";
import dayjs from "dayjs";
import React, { useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { useToggle } from "react-use";

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

const LastSync: React.FC<{ transitionedAt: number | undefined; showRelativeTime: boolean }> = ({
  transitionedAt,
  showRelativeTime,
}) => {
  const lastSyncDisplayText = useMemo(() => {
    if (transitionedAt) {
      const lastSync = dayjs(transitionedAt);

      if (showRelativeTime) {
        return lastSync.fromNow();
      }
      return lastSync.format("MM.DD.YY HH:mm:ss");
    }
    return null;
  }, [transitionedAt, showRelativeTime]);
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

  const { filteredStreams } = useStreamsListContext();

  const streamEntries = filteredStreams.map((stream) => {
    return {
      name: stream.streamName,
      state: stream,
    };
  });

  const columnHelper = useMemo(() => createColumnHelper<(typeof streamEntries)[number]>(), []);
  const columns = useMemo(
    () => [
      columnHelper.accessor("state", {
        id: "statusIcon",
        header: () => <FormattedMessage id="connection.stream.status.table.status" />,
        cell: (props) => (
          <FlexContainer justifyContent="flex-start" gap="sm" alignItems="center">
            <StreamStatusIndicator status={props.cell.getValue().status} loading={props.cell.getValue().isRunning} />
            <FormattedMessage id={`connection.stream.status.${props.cell.getValue().status}`} />
          </FlexContainer>
        ),
        meta: { thClassName: styles.statusHeader },
      }),
      columnHelper.accessor("name", {
        header: () => <FormattedMessage id="connection.stream.status.table.streamName" />,
        cell: (props) => <>{props.cell.getValue()}</>,
      }),
      columnHelper.accessor("state", {
        id: "lastSync",
        header: () => (
          <button onClick={setShowRelativeTime} className={styles.clickableHeader}>
            <FormattedMessage id={`connection.stream.status.table.lastRecord${showRelativeTime ? "" : "At"}`} />
            <TimeIcon />
          </button>
        ),
        cell: (props) => (
          <LastSync transitionedAt={props.cell.getValue()?.lastSuccessfulSyncAt} showRelativeTime={showRelativeTime} />
        ),
      }),
      columnHelper.accessor("state", {
        header: () => null,
        id: "actions",
        cell: (props) => <StreamActionsMenu streamState={props.cell.getValue()} />,
        meta: {
          thClassName: styles.actionsHeader,
        },
      }),
    ],
    [columnHelper, setShowRelativeTime, showRelativeTime]
  );

  return (
    <>
      <Box mb="md">
        <ConnectionStatusCard />
      </Box>
      <Card
        title={
          <FlexContainer justifyContent="space-between" alignItems="center">
            <FormattedMessage id="connection.stream.status.title" />
            <StreamSearchFiltering className={styles.search} />
          </FlexContainer>
        }
      >
        <FlexContainer direction="column" gap="sm">
          <div className={styles.tableContainer} data-survey="streamcentric">
            <Table
              columns={columns}
              data={streamEntries}
              variant="inBlock"
              className={styles.table}
              getRowClassName={(data) => classNames({ [styles.syncing]: data.state?.isRunning })}
            />
          </div>
        </FlexContainer>
      </Card>
    </>
  );
};
