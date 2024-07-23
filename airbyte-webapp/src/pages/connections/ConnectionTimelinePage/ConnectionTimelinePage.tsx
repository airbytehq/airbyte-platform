import { FormattedMessage, useIntl } from "react-intl";
import { useEffectOnce } from "react-use";

import { EmptyState } from "components/common/EmptyState";
import { ConnectionSyncContextProvider } from "components/connection/ConnectionSync/ConnectionSyncContext";
import { PageContainer } from "components/PageContainer";
import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { useFilters, useGetConnectionEvent, useListConnectionEvents } from "core/api";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { trackError } from "core/utils/datadog";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useModalService } from "hooks/services/Modal";

import { ClearEventItem } from "./components/ClearEventItem";
import { JobStartEventItem } from "./components/JobStartEventItem";
import { RefreshEventItem } from "./components/RefreshEventItem";
import { SyncEventItem } from "./components/SyncEventItem";
import { SyncFailEventItem } from "./components/SyncFailEventItem";
import { ConnectionTimelineFilters } from "./ConnectionTimelineFilters";
import styles from "./ConnectionTimelinePage.module.scss";
import { openJobLogsModalFromTimeline } from "./JobEventMenu";
import {
  clearEventSchema,
  jobStartedEventSchema,
  refreshEventSchema,
  syncEventSchema,
  syncFailEventSchema,
} from "./types";
import { eventTypeByStatusFilterValue, TimelineFilterValues } from "./utils";

export const ConnectionTimelinePage: React.FC = () => {
  useTrackPage(PageTrackingCodes.CONNECTIONS_ITEM_TIMELINE);
  const { connection } = useConnectionEditService();

  const { events: connectionEvents } = useListConnectionEvents(connection.connectionId);
  const { openModal } = useModalService();
  const { formatMessage } = useIntl();

  const [filterValues, setFilterValue, resetFilters, filtersAreDefault] = useFilters<TimelineFilterValues>({
    status: null,
    eventType: null,
    eventId: null,
    openLogs: null,
    jobId: null,
    attemptNumber: null,
  });
  const singleEventItem = useGetConnectionEvent(filterValues.eventId);

  // todo: this will move to the endpoint in https://github.com/airbytehq/airbyte-platform-internal/pull/13128
  const connectionEventsToShow = connectionEvents.filter((connectionEvent) => {
    const { eventId: eventIdFilter, status: statusFilter, eventType: eventTypeFilter } = filterValues;

    if (eventIdFilter) {
      return connectionEvent.id === eventIdFilter;
    }

    if (statusFilter) {
      const eventTypes = eventTypeByStatusFilterValue[statusFilter];

      if (eventTypeFilter) {
        return (
          eventTypes.includes(connectionEvent.eventType) &&
          ((eventTypeFilter === "sync" && syncEventSchema.isValidSync(connectionEvent)) ||
            (eventTypeFilter === "refresh" && refreshEventSchema.isValidSync(connectionEvent)) ||
            (eventTypeFilter === "clear" && clearEventSchema.isValidSync(connectionEvent)))
        );
      }

      return eventTypes.includes(connectionEvent.eventType);
    }

    if (eventTypeFilter) {
      if (eventTypeFilter === "sync") {
        return syncEventSchema.isValidSync(connectionEvent);
      } else if (eventTypeFilter === "refresh") {
        return refreshEventSchema.isValidSync(connectionEvent);
      } else if (eventTypeFilter === "clear") {
        return clearEventSchema.isValidSync(connectionEvent);
      }
      return false;
    }

    return true;
  });

  useEffectOnce(() => {
    if (filterValues.openLogs === "true" && (filterValues.eventId || !!singleEventItem?.summary.jobId)) {
      const jobId = singleEventItem?.summary.jobId;

      const jobIdFromFilter = parseInt(filterValues.jobId ?? "");
      const attemptNumberFromFilter = parseInt(filterValues.attemptNumber ?? "");

      openJobLogsModalFromTimeline({
        openModal,
        jobId: !isNaN(jobIdFromFilter) ? jobIdFromFilter : jobId,
        formatMessage,
        connectionName: connection.name ?? "",
        initialAttemptId: !isNaN(attemptNumberFromFilter) ? attemptNumberFromFilter : undefined,
      });
    }
  });

  return (
    <PageContainer centered>
      <ConnectionSyncContextProvider>
        <Box p="xl">
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
                  resetFilters={resetFilters}
                  filtersAreDefault={filtersAreDefault}
                />
              </FlexContainer>
            </Box>
            <div className={styles.eventList}>
              {connectionEventsToShow.length === 0 && (
                <Box p="xl">
                  <EmptyState text={<FormattedMessage id="connection.timeline.empty" />} />
                </Box>
              )}
              {connectionEventsToShow.map((event) => {
                if (syncEventSchema.isValidSync(event, { recursive: true, stripUnknown: true })) {
                  return (
                    <Box py="lg" key={event.id}>
                      <SyncEventItem syncEvent={event} />
                    </Box>
                  );
                } else if (syncFailEventSchema.isValidSync(event, { recursive: true, stripUnknown: true })) {
                  return (
                    <Box py="lg" key={event.id}>
                      <SyncFailEventItem syncEvent={event} />
                    </Box>
                  );
                } else if (refreshEventSchema.isValidSync(event, { recursive: true, stripUnknown: true })) {
                  return (
                    <Box py="lg" key={event.id}>
                      <RefreshEventItem refreshEvent={event} key={event.id} />
                    </Box>
                  );
                } else if (clearEventSchema.isValidSync(event, { recursive: true, stripUnknown: true })) {
                  return (
                    <Box py="lg" key={event.id}>
                      <ClearEventItem clearEvent={event} />
                    </Box>
                  );
                } else if (jobStartedEventSchema.isValidSync(event, { recursive: true, stripUnknown: true })) {
                  return (
                    <Box py="lg" key={event.id}>
                      <JobStartEventItem jobStartEvent={event} />
                    </Box>
                  );
                }
                trackError(new Error("Invalid connection timeline event"), { event });
                return null;
              })}
            </div>
          </Card>
        </Box>
      </ConnectionSyncContextProvider>
    </PageContainer>
  );
};
