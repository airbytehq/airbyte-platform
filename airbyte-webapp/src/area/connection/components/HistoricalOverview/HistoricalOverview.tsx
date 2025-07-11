import dayjs from "dayjs";
import { useMemo, useState } from "react";
import { FormattedMessage } from "react-intl";

import { LoadingPage } from "components";
import { useConnectionStatus } from "components/connection/ConnectionStatus/useConnectionStatus";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Text } from "components/ui/Text";

import {
  useCurrentConnection,
  useGetConnectionDataHistory,
  useGetConnectionSyncProgress,
  useGetConnectionUptimeHistory,
} from "core/api";
import {
  ConnectionSyncProgressRead,
  ConnectionSyncStatus,
  ConnectionUptimeHistoryRead,
} from "core/api/types/AirbyteClient";
import { DefaultErrorBoundary } from "core/errors";

import styles from "./HistoricalOverview.module.scss";
import { NoDataMessage } from "./NoDataMessage";
import { DataMovedGraph } from "../DataMovedGraph";
import {
  CHART_BASE_HEIGHT,
  CHART_MAX_HEIGHT,
  CHART_MIN_HEIGHT,
  CHART_STREAM_ROW_HEIGHT,
} from "../DataMovedGraph/constants";
import { formatDataForChart, UptimeStatusGraph } from "../UptimeStatusGraph";

const generatePlaceholderHistory = (
  connectionSyncProgress?: ConnectionSyncProgressRead
): ConnectionUptimeHistoryRead => {
  if (
    !connectionSyncProgress ||
    connectionSyncProgress.configType === "clear" ||
    connectionSyncProgress.configType === "reset_connection"
  ) {
    return [];
  }

  return [
    {
      bytesCommitted: connectionSyncProgress.bytesCommitted ?? 0,
      bytesEmitted: connectionSyncProgress.bytesCommitted ?? 0,
      configType: connectionSyncProgress.configType,
      jobId: connectionSyncProgress.jobId ?? 0,
      jobCreatedAt: connectionSyncProgress.syncStartedAt ?? dayjs().unix(),
      jobUpdatedAt: dayjs().unix(),
      recordsCommitted: connectionSyncProgress.recordsCommitted ?? 0,
      recordsEmitted: connectionSyncProgress.recordsEmitted ?? 0,
      recordsRejected: connectionSyncProgress.recordsRejected ?? 0,
      streamStatuses: connectionSyncProgress.streams.map((syncProgressItem) => {
        return {
          status: "running",
          streamName: syncProgressItem.streamName,
          streamNamespace: syncProgressItem.streamNamespace ?? "",
        };
      }),
    },
  ];
};

export const HistoricalOverview: React.FC = () => {
  const [statusSyncCount, setStatusSyncCount] = useState(8);
  const [recordsSyncCount, setRecordsSyncCount] = useState(8);

  const connection = useCurrentConnection();

  // generate any placeholder
  const { status } = useConnectionStatus(connection.connectionId);
  const isRunning = status === ConnectionSyncStatus.running;
  const { data: syncProgressData } = useGetConnectionSyncProgress(connection.connectionId, isRunning);
  const placeholderHistory = useMemo(
    () => generatePlaceholderHistory(isRunning ? syncProgressData : undefined),
    [syncProgressData, isRunning]
  );

  // Get and format data for UPTIME STATUS graph
  const { data: uptimeHistoryData, isFetching: uptimeHistoryIsFetching } = useGetConnectionUptimeHistory(
    connection.connectionId,
    statusSyncCount
  );
  const streamHistoryData = useMemo(
    () => [...(uptimeHistoryData ? uptimeHistoryData : []), ...placeholderHistory],
    [uptimeHistoryData, placeholderHistory]
  );
  const hasStreamHistoryData = streamHistoryData.length > 0;
  const { statusData, streamIdentities } = useMemo(() => formatDataForChart(streamHistoryData), [streamHistoryData]);

  // Get and format data for RECORDS graph
  const { data: recordsData, isFetching: recordsIsFetching } = useGetConnectionDataHistory(
    connection.connectionId,
    recordsSyncCount
  );
  const recordsHistoryData = useMemo(
    () => [...(recordsData ? recordsData : []), ...placeholderHistory],
    [recordsData, placeholderHistory]
  );
  const hasRecordsData = recordsHistoryData.length > 0;

  const makeSyncCountButtons = (syncCount: number, setSyncCount: (syncCount: number) => void, isFetching: boolean) => (
    <FlexContainer gap="none" alignItems="center">
      <Button variant="clear" disabled={isFetching} onClick={() => setSyncCount(30)}>
        <Text as="span" color={syncCount !== 30 ? "grey" : undefined} bold={syncCount === 30}>
          <FormattedMessage id="connection.overview.graph.syncsCount" values={{ count: 30 }} />
        </Text>
      </Button>
      <Button variant="clear" disabled={isFetching} onClick={() => setSyncCount(8)}>
        <Text as="span" color={syncCount !== 8 ? "grey" : undefined} bold={syncCount === 8}>
          <FormattedMessage id="connection.overview.graph.syncsCount" values={{ count: 8 }} />
        </Text>
      </Button>
    </FlexContainer>
  );

  const streamsToSizeFor = hasStreamHistoryData
    ? // if there is stream history data, the height comes from the count of unique streams present in the data
      streamIdentities.length
    : // otherwise, the height comes from the count of currently enabled streams
      connection.syncCatalog.streams.filter((stream) => stream.config?.selected).length;

  const chartHeight = Math.max(
    CHART_MIN_HEIGHT,
    Math.min(CHART_MAX_HEIGHT, streamsToSizeFor * CHART_STREAM_ROW_HEIGHT + CHART_BASE_HEIGHT)
  );

  if (!hasStreamHistoryData && !hasRecordsData) {
    if (uptimeHistoryIsFetching || recordsIsFetching) {
      // initial loading state
      return (
        <Box>
          <LoadingPage />
        </Box>
      );
    }

    // should only happen for a connection without any syncs
    return (
      <Box m="xl">
        <NoDataMessage />
      </Box>
    );
  }

  return (
    <div className={styles.container}>
      <FlexContainer className={styles.cardLayout}>
        <Card className={styles.graphCard}>
          <FlexContainer direction="column">
            <FlexContainer justifyContent="space-between">
              <Heading as="h5" size="sm">
                <FormattedMessage id="connection.overview.graph.uptimeStatus" />
              </Heading>
              {makeSyncCountButtons(statusSyncCount, setStatusSyncCount, uptimeHistoryIsFetching)}
            </FlexContainer>
            <DefaultErrorBoundary>
              <UptimeStatusGraph data={statusData} height={chartHeight} />
              {uptimeHistoryIsFetching && (
                <div className={styles.loadingState}>
                  <LoadingPage className={styles.loadingPage} />
                </div>
              )}
            </DefaultErrorBoundary>
          </FlexContainer>
        </Card>

        <Card className={styles.graphCard}>
          <FlexContainer direction="column">
            <FlexContainer justifyContent="space-between">
              <Heading as="h5" size="sm">
                <FormattedMessage id="connection.overview.graph.dataMoved" />
              </Heading>
              {makeSyncCountButtons(recordsSyncCount, setRecordsSyncCount, recordsIsFetching)}
            </FlexContainer>
            <DefaultErrorBoundary>
              <DataMovedGraph data={recordsHistoryData} height={chartHeight} />
              {recordsIsFetching && (
                <div className={styles.loadingState}>
                  <LoadingPage className={styles.loadingPage} />
                </div>
              )}
            </DefaultErrorBoundary>
          </FlexContainer>
        </Card>
      </FlexContainer>
    </div>
  );
};
