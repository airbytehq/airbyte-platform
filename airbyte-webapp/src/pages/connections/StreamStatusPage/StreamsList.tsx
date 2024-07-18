import { createColumnHelper } from "@tanstack/react-table";
import classNames from "classnames";
import { useRef, forwardRef, useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { useToggle } from "react-use";

import { useConnectionStatus } from "components/connection/ConnectionStatus/useConnectionStatus";
import { ConnectionStatusIndicatorStatus } from "components/connection/ConnectionStatusIndicator";
import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { Table } from "components/ui/Table";

import { activeStatuses } from "area/connection/utils";
import { useTrackSyncProgress } from "area/connection/utils/useStreamsTableAnalytics";
import { useUiStreamStates } from "area/connection/utils/useUiStreamsStates";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";

import { DataFreshnessCell } from "./DataFreshnessCell";
import { LatestSyncCell } from "./LatestSyncCell";
import { StreamActionsMenu } from "./StreamActionsMenu";
import { StreamSearchFiltering } from "./StreamSearchFiltering";
import styles from "./StreamsList.module.scss";
import { StatusCell } from "./StreamsListStatusCell";
import { StreamsListSubtitle } from "./StreamsListSubtitle";

export const StreamsList = forwardRef<HTMLDivElement>((_, outerRef) => {
  const [showRelativeTime, setShowRelativeTime] = useToggle(true);
  const { connection } = useConnectionEditService();
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
        cell: (props) => <>{props.cell.getValue()}</>,
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
          <button onClick={setShowRelativeTime} className={styles.clickableHeader}>
            <FormattedMessage id="connection.stream.status.table.dataFreshAsOf" />
            <Icon type="clockOutline" size="sm" className={styles.icon} />
          </button>
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

  const { status, nextSync, recordsExtracted, recordsLoaded } = useConnectionStatus(connection.connectionId);

  const customScrollParent =
    typeof outerRef !== "function" && outerRef && outerRef.current ? outerRef.current : undefined;

  return (
    <Card noPadding>
      <Box p="xl" className={styles.cardHeader}>
        <FlexContainer justifyContent="space-between" alignItems="center">
          <FlexContainer alignItems="center">
            <Heading as="h5" size="sm">
              <FormattedMessage id="connection.stream.status.title" />
            </Heading>
            <StreamsListSubtitle
              connectionStatus={status}
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
          getRowClassName={(data) =>
            classNames(styles.row, {
              [styles["syncing--next"]]:
                activeStatuses.includes(data.status) && data.status !== ConnectionStatusIndicatorStatus.Queued,
            })
          }
          sorting={false}
          virtualized
          virtualizedProps={{ customScrollParent, useWindowScroll: true }}
        />
      </FlexContainer>
    </Card>
  );
});
StreamsList.displayName = "StreamsList";
