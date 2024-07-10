import { FormattedMessage, useIntl } from "react-intl";

import { ConnectionSyncContextProvider } from "components/connection/ConnectionSync/ConnectionSyncContext";
import { PageContainer } from "components/PageContainer";
import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { useFilters } from "core/api";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useModalService } from "hooks/services/Modal";

import { ClearEvent } from "./components/ClearEvent";
import { RefreshEvent } from "./components/RefreshEvent";
import { SyncEvent } from "./components/SyncEvent";
import { ConnectionTimelineFilters } from "./ConnectionTimelineFilters";
import styles from "./ConnectionTimelinePage.module.scss";
import { openJobLogsModalFromTimeline } from "./JobEventMenu";
import { mockConnectionTimelineEventList } from "./mocks";
import {
  castEventSummaryToConnectionTimelineJobStatsProps,
  eventTypeByFilterValue,
  eventTypeByStatusFilterValue,
  extractStreamsFromTimelineEvent,
  TimelineFilterValues,
} from "./utils";

export const ConnectionTimelinePage: React.FC = () => {
  useTrackPage(PageTrackingCodes.CONNECTIONS_ITEM_TIMELINE);
  const { events: connectionEvents } = mockConnectionTimelineEventList;
  const { openModal } = useModalService();
  const { formatMessage } = useIntl();
  const { connection } = useConnectionFormService();

  const [filterValues, setFilterValue, setFilters] = useFilters<TimelineFilterValues>({
    status: null,
    eventType: null,
    eventId: null,
    openLogs: null,
  });

  const connectionEventsToShow = connectionEvents.filter((connectionEvent) => {
    if (filterValues.eventId) {
      return connectionEvent.id === filterValues.eventId;
    }

    if (filterValues.status && filterValues.eventType) {
      return (
        eventTypeByStatusFilterValue[filterValues.status].includes(connectionEvent.eventType) &&
        eventTypeByFilterValue[filterValues.eventType].includes(connectionEvent.eventType)
      );
    }
    if (filterValues.status) {
      return eventTypeByStatusFilterValue[filterValues.status].includes(connectionEvent.eventType);
    }
    if (filterValues.eventType) {
      return eventTypeByFilterValue[filterValues.eventType].includes(connectionEvent.eventType);
    }

    return true;
  });

  if (filterValues.openLogs && filterValues.eventId) {
    const jobId = 55874; // todo: calculate this by fetching single connection timeline event from API based on filterValues.eventId once get endpoint is merged!
    openJobLogsModalFromTimeline(openModal, jobId, formatMessage, connection.name ?? "");
  }

  return (
    <PageContainer centered>
      <ConnectionSyncContextProvider>
        <Box pb="xl">
          <Card noPadding>
            <Box p="lg">
              <FlexContainer direction="column">
                <FlexContainer justifyContent="space-between" alignItems="center">
                  <Heading as="h5" size="sm">
                    <FormattedMessage id="connection.timeline" />
                  </Heading>
                </FlexContainer>
                <ConnectionTimelineFilters
                  filterValues={filterValues}
                  setFilterValue={setFilterValue}
                  setFilters={setFilters}
                />
              </FlexContainer>
            </Box>
            <div className={styles.eventList}>
              {connectionEventsToShow.map((event) => {
                const stats = castEventSummaryToConnectionTimelineJobStatsProps(event.eventSummary);
                const streamsToList = extractStreamsFromTimelineEvent(event.eventSummary);

                if (!stats || stats.jobStatus === "pending" || stats.jobStatus === "running") {
                  return null;
                }

                return (
                  <Box py="lg" key={event.id}>
                    {event.eventType.includes("sync") && (
                      <SyncEvent
                        jobId={stats.jobId}
                        recordsCommitted={stats.recordsCommitted}
                        bytesCommitted={stats.bytesCommitted}
                        eventId={event.id}
                        jobStartedAt={stats.jobStartedAt}
                        jobEndedAt={stats.jobEndedAt}
                        eventType={event.eventType}
                        attemptsCount={stats.attemptsCount}
                        jobStatus={stats.jobStatus}
                        failureMessage={stats.failureMessage}
                      />
                    )}
                    {event.eventType.includes("clear") && (
                      <ClearEvent
                        jobId={stats.jobId}
                        eventId={event.id}
                        eventType={event.eventType}
                        attemptsCount={stats.attemptsCount}
                        jobStatus={stats.jobStatus}
                        clearedStreams={streamsToList}
                      />
                    )}
                    {event.eventType.includes("refresh") && (
                      <RefreshEvent
                        jobId={stats.jobId}
                        eventId={event.id}
                        eventType={event.eventType}
                        attemptsCount={stats.attemptsCount}
                        jobStatus={stats.jobStatus}
                        refreshedStreams={streamsToList}
                      />
                    )}
                  </Box>
                );
              })}
            </div>
          </Card>
        </Box>
      </ConnectionSyncContextProvider>
    </PageContainer>
  );
};
