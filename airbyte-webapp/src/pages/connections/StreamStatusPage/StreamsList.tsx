import { createColumnHelper } from "@tanstack/react-table";
import classNames from "classnames";
import { useRef, useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { useToggle } from "react-use";

import { useConnectionStatus } from "components/connection/ConnectionStatus/useConnectionStatus";
import { StreamStatusType } from "components/connection/StreamStatusIndicator";
import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { Table } from "components/ui/Table";
import { Tooltip } from "components/ui/Tooltip";

import { activeStatuses } from "area/connection/utils";
import { useTrackSyncProgress } from "area/connection/utils/useStreamsTableAnalytics";
import { useUiStreamStates } from "area/connection/utils/useUiStreamsStates";
import { useCurrentConnection } from "core/api";

import { DataFreshnessCell } from "./DataFreshnessCell";
import { LatestSyncCell } from "./LatestSyncCell";
import { StreamActionsMenu } from "./StreamActionsMenu";
import { StreamSearchFiltering } from "./StreamSearchFiltering";
import styles from "./StreamsList.module.scss";
import { StatusCell } from "./StreamsListStatusCell";
import { StreamsListSubtitle } from "./StreamsListSubtitle";

export const StreamsList: React.FC<{ customScrollParent: HTMLElement | null }> = ({ customScrollParent }) => {
  const [showRelativeTime, setShowRelativeTime] = useToggle(true);
  const connection = useCurrentConnection();
  const streamEntries = useUiStreamStates(connection.connectionId);
  const trackCountRef = useRef(0);
  useTrackSyncProgress(connection.connectionId, trackCountRef);

  const columnHelper = useMemo(() => createColumnHelper<(typeof streamEntries)[number]>(), []);

  const columns = useMemo(
    () => [
      columnHelper.accessor("status", {
        id: "statusIcon",
        header: () => <FormattedMessage id="connection.stream.status.table.status" />,
        cell: StatusCell,
        meta: { thClassName: styles.statusHeader },
      }),
      columnHelper.accessor("streamName", {
        header: () => <FormattedMessage id="connection.stream.status.table.streamName" />,
        cell: (props) => <span data-testid="streams-list-name-cell-content">{props.cell.getValue()}</span>,
        meta: { responsive: true },
      }),
      columnHelper.accessor("recordsLoaded", {
        id: "latestSync",
        header: () => <FormattedMessage id="connection.stream.status.table.latestSync" />,
        cell: (props) => {
          return (
            <LatestSyncCell
              recordsLoaded={props.row.original.recordsLoaded}
              recordsExtracted={props.row.original.recordsExtracted}
              syncStartedAt={props.row.original.activeJobStartedAt}
              status={props.row.original.status}
              isLoadingHistoricalData={props.row.original.isLoadingHistoricalData}
            />
          );
        },
        meta: { responsive: true },
      }),
      columnHelper.accessor("dataFreshAsOf", {
        header: () => (
          <Tooltip
            placement="top"
            control={
              <button onClick={setShowRelativeTime} className={styles.clickableHeader}>
                <FormattedMessage id="connection.stream.status.table.dataFreshAsOf" />
                <Icon type="clockOutline" size="sm" className={styles.icon} />
              </button>
            }
          >
            <FormattedMessage
              id={
                showRelativeTime
                  ? "connection.stream.status.table.dataFreshAsOf.absolute"
                  : "connection.stream.status.table.dataFreshAsOf.relative"
              }
            />
          </Tooltip>
        ),
        cell: (props) => (
          <DataFreshnessCell transitionedAt={props.cell.getValue()} showRelativeTime={showRelativeTime} />
        ),
        meta: { responsive: true },
      }),
      columnHelper.accessor("dataFreshAsOf", {
        header: () => null,
        id: "actions",
        cell: (props) => (
          <StreamActionsMenu
            streamName={props.row.original.streamName}
            streamNamespace={props.row.original.streamNamespace}
          />
        ),
        meta: {
          thClassName: styles.actionsHeader,
        },
      }),
    ],
    [columnHelper, setShowRelativeTime, showRelativeTime]
  );

  const {
    status: connectionStatus,
    nextSync,
    recordsExtracted,
    recordsLoaded,
  } = useConnectionStatus(connection.connectionId);

  return (
    <Card noPadding>
      <Box p="xl" className={styles.cardHeader}>
        <FlexContainer justifyContent="space-between" alignItems="center">
          <FlexContainer alignItems="flex-end">
            <Heading as="h5" size="sm">
              <FormattedMessage id="connection.stream.status.title" />
            </Heading>
            <StreamsListSubtitle
              connectionStatus={connectionStatus}
              nextSync={nextSync}
              recordsLoaded={recordsLoaded}
              recordsExtracted={recordsExtracted}
            />
          </FlexContainer>

          <StreamSearchFiltering className={styles.search} />
        </FlexContainer>
      </Box>
      <FlexContainer direction="column" gap="sm" className={styles.tableContainer} data-survey="streamcentric">
        <Table
          columns={columns}
          data={streamEntries}
          variant="inBlock"
          className={styles.table}
          rowId={(row) => `${row.streamNamespace ?? ""}.${row.streamName}`}
          getRowClassName={(stream) =>
            classNames(styles.row, {
              [styles["syncing--next"]]:
                activeStatuses.includes(stream.status) && stream.status !== StreamStatusType.Queued,
            })
          }
          sorting={false}
          virtualized
          virtualizedProps={{ customScrollParent: customScrollParent ?? undefined, useWindowScroll: true }}
        />
      </FlexContainer>
    </Card>
  );
};
