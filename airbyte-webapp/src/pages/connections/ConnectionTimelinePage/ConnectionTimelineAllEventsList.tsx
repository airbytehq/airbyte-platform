import { HTMLAttributes, Ref, forwardRef, useContext, useEffect, useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { Virtuoso } from "react-virtuoso";
import { z } from "zod";

import { LoadingPage } from "components";
import { useConnectionStatus } from "components/connection/ConnectionStatus/useConnectionStatus";
import { EmptyState } from "components/EmptyState";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { LoadingSpinner } from "components/ui/LoadingSpinner";
import { ScrollParentContext } from "components/ui/ScrollParent";

import { useCurrentConnection, useGetConnectionSyncProgress, useListConnectionEventsInfinite } from "core/api";
import { ConnectionEvent, ConnectionEventSummary, ConnectionSyncStatus } from "core/api/types/AirbyteClient";
import { trackError } from "core/utils/datadog";

import styles from "./ConnectionTimelineAllEventsList.module.scss";
import { ConnectionTimelineRunningEvent, EventTypeToSchema, eventTypeToSchemaMap } from "./types";
import {
  eventTypeByStatusFilterValue,
  TimelineFilterValues,
  eventTypeByTypeFilterValue,
  createRunningEventFromJob,
} from "./utils";

const onlyResourceRequirements = z
  .object({
    patches: z
      .object({
        resourceRequirements: z.unknown().optional(),
      })
      .strict()
      .optional(),
  })
  .strict();

export const validateAndMapEvent = (event: ConnectionEvent | ConnectionTimelineRunningEvent) => {
  /**
   * known cases for excluding timeline events that we should not trigger error reporting for:
   * - events with only resourceRequirement patches
   * - events created prior to July 20 are not guaranteed to be valid
   */
  const isEventRecent = (createdAt: number | undefined): boolean => {
    const threshold = 1721433600;
    return !createdAt || createdAt > threshold;
  };

  const hasOnlyResourceRequirementPatches = (summary: ConnectionEventSummary): boolean =>
    onlyResourceRequirements.safeParse(summary).success;

  const eventType = event.eventType as string; // eventType can be out of sync with the eventTypeToSchemaMap

  if (eventType in eventTypeToSchemaMap) {
    const { schema, component: Component } = eventTypeToSchemaMap[eventType as keyof EventTypeToSchema];
    const result = schema.safeParse(event);

    if (result.success) {
      const EventComponent = Component as React.FC<{ event: typeof result.data }>;

      return (
        <Box py="lg" key={event.id}>
          <EventComponent event={result.data} />
        </Box>
      );
    }
  }

  if (isEventRecent(event.createdAt) && !hasOnlyResourceRequirementPatches(event.summary)) {
    trackError(new Error("Invalid connection timeline event"), { event });
  }

  return null;
};

// Virtuoso's `List` ref is an HTMLDivElement so we're coercing some types here
const UlList = forwardRef<HTMLDivElement>((props, ref) => (
  <ul
    ref={ref as Ref<HTMLUListElement>}
    {...(props as HTMLAttributes<HTMLUListElement>)}
    className={styles.eventList}
  />
));
UlList.displayName = "UlList";

export const ConnectionTimelineAllEventsList: React.FC<{
  filterValues: TimelineFilterValues;
}> = ({ filterValues }) => {
  const customScrollParent = useContext(ScrollParentContext);
  const connection = useCurrentConnection();
  const { status } = useConnectionStatus(connection.connectionId);
  const { data: syncProgressData } = useGetConnectionSyncProgress(
    connection.connectionId,
    status === ConnectionSyncStatus.running
  );

  const eventTypesToFetch = useMemo(() => {
    const statusEventTypes = filterValues.status !== "" ? eventTypeByStatusFilterValue[filterValues.status] : [];
    const typeEventTypes = filterValues.eventCategory ? eventTypeByTypeFilterValue[filterValues.eventCategory] : [];

    if (filterValues.status !== "" && filterValues.eventCategory !== "") {
      return statusEventTypes.filter((eventType) => typeEventTypes.includes(eventType));
    }
    return [...statusEventTypes, ...typeEventTypes];
  }, [filterValues.eventCategory, filterValues.status]);

  const {
    data: connectionEventsData,
    hasNextPage,
    fetchNextPage,
    isLoading,
    isFetchingNextPage,
    refetch,
  } = useListConnectionEventsInfinite({
    connectionId: connection.connectionId,
    eventTypes: eventTypesToFetch.length > 0 ? eventTypesToFetch : undefined, // only send these if there is a filter set... otherwise fetch everything
    createdAtStart: filterValues.startDate !== "" ? filterValues.startDate : undefined,
    createdAtEnd: filterValues.endDate !== "" ? filterValues.endDate : undefined,
  });

  const endDateShowRunningJob =
    filterValues.endDate === "" || parseInt(filterValues.endDate) > (syncProgressData?.syncStartedAt ?? 0);
  const startDateShowRunningJob =
    filterValues.startDate === "" || parseInt(filterValues.startDate) < (syncProgressData?.syncStartedAt ?? 0);
  const statusFilterShowRunningJob = filterValues.status === ""; // we don't have running status in the filter
  const eventFilterShowRunningJob =
    filterValues.eventCategory === "" ||
    filterValues.eventCategory === syncProgressData?.configType ||
    (filterValues.eventCategory === "clear" && syncProgressData?.configType === "reset_connection");

  const filtersShouldShowRunningJob =
    startDateShowRunningJob && endDateShowRunningJob && statusFilterShowRunningJob && eventFilterShowRunningJob;

  const isRunning = status === ConnectionSyncStatus.running;

  const showRunningJob = isRunning && !!syncProgressData && filtersShouldShowRunningJob;

  const runningEvent = useMemo(
    () =>
      showRunningJob && !!syncProgressData?.jobId && !!syncProgressData?.syncStartedAt
        ? createRunningEventFromJob(syncProgressData, connection.connectionId)
        : null,
    [showRunningJob, syncProgressData, connection.connectionId]
  );

  const connectionEventsToShow = useMemo(
    () => [
      ...(runningEvent ? [runningEvent] : []),
      ...(connectionEventsData?.pages.flatMap<ConnectionEvent>((page) => page.data.events) ?? []),
    ],
    [connectionEventsData?.pages, runningEvent]
  );

  const validatedEvents = connectionEventsToShow
    .map((event) => validateAndMapEvent(event))
    .filter((event) => event !== null);

  useEffect(() => {
    refetch();
  }, [isRunning, refetch]);

  const handleEndReached = () => {
    if (hasNextPage) {
      fetchNextPage();
    }
  };

  if (isLoading) {
    return <LoadingPage />;
  }

  if (validatedEvents.length === 0) {
    return (
      <Box p="xl">
        <EmptyState text={<FormattedMessage id="connection.timeline.empty" />} />
      </Box>
    );
  }

  return (
    <>
      <Virtuoso
        data={validatedEvents}
        customScrollParent={customScrollParent ?? undefined}
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
          return event;
        }}
      />
      {isFetchingNextPage && (
        <FlexContainer alignItems="center" justifyContent="center">
          <Box py="md">
            <LoadingSpinner />
          </Box>
        </FlexContainer>
      )}
    </>
  );
};
