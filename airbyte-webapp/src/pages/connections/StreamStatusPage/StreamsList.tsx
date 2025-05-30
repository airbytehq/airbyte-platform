import { createColumnHelper } from "@tanstack/react-table";
import classNames from "classnames";
import React, { useRef, useMemo, useContext, useState } from "react";
import { FormattedMessage } from "react-intl";

import { useConnectionStatus } from "components/connection/ConnectionStatus/useConnectionStatus";
import { StreamStatusType } from "components/connection/StreamStatusIndicator";
import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { ScrollParentContext } from "components/ui/ScrollParent";
import { SearchInput } from "components/ui/SearchInput";
import { Table } from "components/ui/Table";

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
import { SyncMetricOption, SyncMetricListbox } from "./SyncMetricListbox";

export const StreamsList: React.FC = () => {
  const [syncMetric, setSyncMetric] = useState<SyncMetricOption>(SyncMetricOption.records);

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
          <FlexContainer alignItems="baseline" gap="sm">
            <FormattedMessage id="connection.stream.status.table.latestSync" />
            <SyncMetricListbox selectedValue={syncMetric} onSelect={setSyncMetric} />
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
              showBytes={syncMetric === SyncMetricOption.bytes}
            />
          );
        },
        meta: {
          thClassName: styles.latestSyncHeader,
        },
      }),
      columnHelper.accessor("dataFreshAsOf", {
        header: () => <FormattedMessage id="connection.stream.status.table.dataFreshAsOf" />,
        cell: (props) => <DataFreshnessCell transitionedAt={props.cell.getValue()} />,
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
    [columnHelper, syncMetric]
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
