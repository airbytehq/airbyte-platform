import { createColumnHelper } from "@tanstack/react-table";
import classNames from "classnames";
import dayjs from "dayjs";
import React, { useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { useToggle } from "react-use";

import { useConnectionStatus } from "components/connection/ConnectionStatus/useConnectionStatus";
import { ConnectionStatusIndicatorStatus } from "components/connection/ConnectionStatusIndicator";
import { StreamStatusIndicator } from "components/connection/StreamStatusIndicator";
import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { Table } from "components/ui/Table";
import { Text } from "components/ui/Text";

import { ConnectionStatus } from "core/api/types/AirbyteClient";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useExperiment } from "hooks/services/Experiment";

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
  const useSimpliedCreation = useExperiment("connection.simplifiedCreation", false);
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
          <FlexContainer justifyContent="flex-start" gap="sm" alignItems="center" className={styles.statusCell}>
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
            <Icon type="clockOutline" size="sm" className={styles.icon} />
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

  const { connection } = useConnectionEditService();
  const { status, nextSync } = useConnectionStatus(connection.connectionId);

  const showTable = connection.status !== ConnectionStatus.inactive;

  return (
    <Card noPadding>
      <Box p="xl" className={styles.cardHeader}>
        <FlexContainer justifyContent="space-between" alignItems="center">
          {useSimpliedCreation ? (
            <FlexContainer alignItems="center">
              <Heading as="h5" size="sm">
                <FormattedMessage id="connection.stream.status.title" />
              </Heading>
              <Box as="span" ml="md">
                <Text color="grey" bold size="sm" as="span">
                  {status === ConnectionStatusIndicatorStatus.OnTime && nextSync && (
                    <FormattedMessage
                      id="connection.stream.status.nextSync"
                      values={{ sync: dayjs(nextSync).fromNow() }}
                    />
                  )}
                  {(status === ConnectionStatusIndicatorStatus.Late ||
                    status === ConnectionStatusIndicatorStatus.OnTrack) &&
                    nextSync && (
                      <FormattedMessage
                        id="connection.stream.status.nextTry"
                        values={{ sync: dayjs(nextSync).fromNow() }}
                      />
                    )}
                </Text>
              </Box>
            </FlexContainer>
          ) : (
            <Heading as="h5" size="sm">
              <FormattedMessage id="connection.stream.status.title" />
            </Heading>
          )}
          <StreamSearchFiltering className={styles.search} />
        </FlexContainer>
      </Box>
      <FlexContainer direction="column" gap="sm">
        <div className={styles.tableContainer} data-survey="streamcentric">
          {showTable && (
            <Table
              columns={columns}
              data={streamEntries}
              variant="inBlock"
              getRowClassName={(data) => classNames({ [styles.syncing]: data.state?.isRunning })}
              sorting={false}
            />
          )}

          {!showTable && (
            <Box p="xl">
              <Text size="sm" color="grey" italicized>
                <FormattedMessage id="connection.stream.status.table.emptyTable.message" />
              </Text>
            </Box>
          )}
        </div>
      </FlexContainer>
    </Card>
  );
};
