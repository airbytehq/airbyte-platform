import { createColumnHelper } from "@tanstack/react-table";
import classNames from "classnames";
import { useRef, useMemo, useContext, useState } from "react";
import { FormattedMessage } from "react-intl";
import { useToggle } from "react-use";

import { useConnectionStatus } from "components/connection/ConnectionStatus/useConnectionStatus";
import { StreamStatusType } from "components/connection/StreamStatusIndicator";
import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { ScrollParentContext } from "components/ui/ScrollParent";
import { SearchInput } from "components/ui/SearchInput";
import { Table } from "components/ui/Table";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { activeStatuses } from "area/connection/utils";
import { useTrackSyncProgress } from "area/connection/utils/useStreamsTableAnalytics";
import { useUiStreamStates } from "area/connection/utils/useUiStreamsStates";
import { useCurrentConnection } from "core/api";

import { DataFreshnessCell } from "./DataFreshnessCell";
import { LatestSyncCell } from "./LatestSyncCell";
import { StreamActionsMenu } from "./StreamActionsMenu";
import styles from "./StreamsList.module.scss";
import { StatusCell } from "./StreamsListStatusCell";
import { StreamsListSubtitle } from "./StreamsListSubtitle";

export const StreamsList: React.FC = () => {
  const [showRelativeTime, setShowRelativeTime] = useToggle(true);
  const [showBytes, setShowBytes] = useToggle(false);

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
      columnHelper.accessor("streamNameWithPrefix", {
        header: () => <FormattedMessage id="connection.stream.status.table.streamName" />,
        cell: (props) => (
          <div data-testid="streams-list-name-cell-content" className={styles.nameContent}>
            {props.cell.getValue()}
          </div>
        ),
      }),
      columnHelper.accessor("recordsLoaded", {
        id: "latestSync",
        header: () => (
          <FlexContainer alignItems="baseline" gap="none">
            <FormattedMessage
              id="connection.stream.status.table.latestSync"
              values={{
                denomination: (
                  <Tooltip
                    placement="top"
                    control={
                      <button className={styles.clickableHeader} onClick={setShowBytes}>
                        <Text color="grey" size="sm">
                          {showBytes ? (
                            <FormattedMessage id="connection.stream.status.table.latestSync.bytes" />
                          ) : (
                            <FormattedMessage id="connection.stream.status.table.latestSync.records" />
                          )}
                        </Text>
                      </button>
                    }
                  >
                    <FormattedMessage
                      id={
                        showBytes
                          ? "connection.stream.status.table.latestSync.showRecords"
                          : "connection.stream.status.table.latestSync.showBytes"
                      }
                    />
                  </Tooltip>
                ),
              }}
            />
          </FlexContainer>
        ),
        cell: (props) => {
          return (
            <LatestSyncCell
              recordsLoaded={props.row.original.recordsLoaded}
              recordsExtracted={props.row.original.recordsExtracted}
              bytesLoaded={props.row.original.bytesLoaded}
              bytesExtracted={props.row.original.bytesExtracted}
              syncStartedAt={props.row.original.activeJobStartedAt}
              status={props.row.original.status}
              isLoadingHistoricalData={props.row.original.isLoadingHistoricalData}
              showBytes={showBytes}
            />
          );
        },
        meta: {
          thClassName: styles.latestSyncHeader,
        },
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
        meta: {
          thClassName: styles.dataFreshAsOfHeader,
        },
      }),
      columnHelper.accessor("dataFreshAsOf", {
        header: () => null,
        id: "actions",
        cell: (props) => (
          <StreamActionsMenu
            streamName={props.row.original.streamName}
            streamNamespace={props.row.original.streamNamespace}
            catalogStream={props.row.original.catalogStream}
          />
        ),
        meta: {
          thClassName: styles.actionsHeader,
        },
      }),
    ],
    [columnHelper, setShowBytes, setShowRelativeTime, showBytes, showRelativeTime]
  );

  const {
    status: connectionStatus,
    nextSync,
    recordsExtracted,
    recordsLoaded,
  } = useConnectionStatus(connection.connectionId);

  const [filtering, setFiltering] = useState("");
  const customScrollParent = useContext(ScrollParentContext);

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
          <div className={styles.search}>
            <SearchInput value={filtering} onChange={({ target: { value } }) => setFiltering(value)} />
          </div>
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
              [styles.syncing]: activeStatuses.includes(stream.status) && stream.status !== StreamStatusType.Queued,
            })
          }
          sorting={false}
          columnFilters={[{ id: "streamNameWithPrefix", value: filtering }]}
          virtualized
          virtualizedProps={{ customScrollParent: customScrollParent ?? undefined, useWindowScroll: true }}
        />
      </FlexContainer>
    </Card>
  );
};
