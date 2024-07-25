import { forwardRef, HTMLAttributes, Ref, useMemo, useRef } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useEffectOnce } from "react-use";
import { Virtuoso } from "react-virtuoso";

import { LoadingPage } from "components";
import { EmptyState } from "components/common/EmptyState";
import { ConnectionSyncContextProvider } from "components/connection/ConnectionSync/ConnectionSyncContext";
import { PageContainer } from "components/PageContainer";
import { ScrollableContainer } from "components/ScrollableContainer";
import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { LoadingSpinner } from "components/ui/LoadingSpinner";

import { useFilters, useGetConnectionEvent, useListConnectionEventsInfinite } from "core/api";
import { ConnectionEvent } from "core/api/types/AirbyteClient";
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
  syncFailEventSchema,
  jobStartedEventSchema,
  refreshEventSchema,
  syncEventSchema,
} from "./types";
import { eventTypeByStatusFilterValue, TimelineFilterValues, eventTypeByTypeFilterValue } from "./utils";

// Virtuoso's `List` ref is an HTMLDivElement so we're coercing some types here
const UlList = forwardRef<HTMLDivElement>((props, ref) => (
  <ul
    ref={ref as Ref<HTMLUListElement>}
    {...(props as HTMLAttributes<HTMLUListElement>)}
    className={styles.eventList}
  />
));
UlList.displayName = "UlList";

export const ConnectionTimelinePage = () => {
  const ref = useRef<HTMLDivElement>(null);
  useTrackPage(PageTrackingCodes.CONNECTIONS_ITEM_TIMELINE);
  const { connection } = useConnectionEditService();

  const { openModal } = useModalService();
  const { formatMessage } = useIntl();

  const [filterValues, setFilterValue, resetFilters, filtersAreDefault] = useFilters<TimelineFilterValues>({
    status: "",
    eventCategory: "",
    startDate: "",
    endDate: "",
    eventId: "",
    openLogs: "",
    jobId: "",
    attemptNumber: "",
  });

  const eventTypesToFetch = useMemo(() => {
    const statusEventTypes = filterValues.status !== "" ? eventTypeByStatusFilterValue[filterValues.status] : [];
    const typeEventTypes = filterValues.eventCategory ? eventTypeByTypeFilterValue[filterValues.eventCategory] : [];

    if (filterValues.status !== "" && filterValues.eventCategory !== "") {
      return statusEventTypes.filter((eventType) => typeEventTypes.includes(eventType));
    }

    return [...statusEventTypes, ...typeEventTypes];
  }, [filterValues]);

  const {
    data: connectionEventsData,
    hasNextPage,
    fetchNextPage,
    isLoading,
    isFetchingNextPage,
  } = useListConnectionEventsInfinite({
    connectionId: connection.connectionId,
    eventTypes: eventTypesToFetch.length > 0 ? eventTypesToFetch : undefined, // only send these if there is a filter set... otherwise fetch everything
    createdAtStart: filterValues.startDate !== "" ? filterValues.startDate : undefined,
    createdAtEnd: filterValues.endDate !== "" ? filterValues.endDate : undefined,
  });
  const connectionEventsToShow = connectionEventsData?.pages.flatMap<ConnectionEvent>((page) => page.data.events) ?? [];

  const singleEventItem = useGetConnectionEvent(filterValues.eventId);

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

  const handleEndReached = () => {
    if (hasNextPage) {
      fetchNextPage();
    }
  };

  return (
    <ScrollableContainer ref={ref}>
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
                    resetFilters={resetFilters}
                    filtersAreDefault={filtersAreDefault}
                  />
                </FlexContainer>
              </Box>
              {isLoading && <LoadingPage />}
              {!isLoading && connectionEventsToShow.length === 0 && (
                <Box p="xl">
                  <EmptyState text={<FormattedMessage id="connection.timeline.empty" />} />
                </Box>
              )}
              {!isLoading && (
                <Virtuoso
                  data={connectionEventsToShow}
                  customScrollParent={!!ref.current ? ref.current : undefined}
                  useWindowScroll
                  endReached={handleEndReached}
                  components={{
                    List: UlList,

                    // components are overly constrained to be a function/class component
                    // but the string representation is fine; react-virtuoso defaults Item to `"div"`
                    // eslint-disable-next-line @typescript-eslint/no-explicit-any
                    Item: "li" as any,
                  }}
                  itemContent={(_index, event) => {
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
                  }}
                />
              )}
              {isFetchingNextPage && (
                <FlexContainer alignItems="center" justifyContent="center">
                  <Box py="md">
                    <LoadingSpinner />
                  </Box>
                </FlexContainer>
              )}
            </Card>
          </Box>
        </ConnectionSyncContextProvider>
      </PageContainer>
    </ScrollableContainer>
  );
};
