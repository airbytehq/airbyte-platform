import { HTMLAttributes, Ref, forwardRef, useEffect, useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { Virtuoso } from "react-virtuoso";

import { LoadingPage } from "components";
import { useConnectionStatus } from "components/connection/ConnectionStatus/useConnectionStatus";
import { EmptyState } from "components/EmptyState";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { LoadingSpinner } from "components/ui/LoadingSpinner";

import { useCurrentConnection, useGetConnectionSyncProgress, useListConnectionEventsInfinite } from "core/api";
import { ConnectionEvent, ConnectionSyncStatus } from "core/api/types/AirbyteClient";

import { EventLineItem } from "./components/EventLineItem";
import styles from "./ConnectionTimelineAllEventsList.module.scss";
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

export const ConnectionTimelineAllEventsList: React.FC<{
  filterValues: TimelineFilterValues;
  scrollElement: HTMLDivElement | null;
}> = ({ filterValues, scrollElement }) => {
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
  const statusFilterShowRunningJob =
    filterValues.status === "" ||
    filterValues.eventCategory === syncProgressData?.configType ||
    (filterValues.eventCategory === "clear" && syncProgressData?.configType === "reset_connection");

  const filtersShouldShowRunningJob = startDateShowRunningJob && endDateShowRunningJob && statusFilterShowRunningJob;

  const isRunning = status === ConnectionSyncStatus.running;

  const showRunningJob = isRunning && !!syncProgressData && filtersShouldShowRunningJob;

  const connectionEventsToShow = useMemo(() => {
    const events = [
      ...(showRunningJob && !!syncProgressData.jobId && !!syncProgressData.syncStartedAt
        ? [
            {
              id: "running",
              eventType: "RUNNING_JOB",
              connectionId: connection.connectionId,
              createdAt: syncProgressData.syncStartedAt ?? Date.now() / 1000,
              summary: {
                streams: syncProgressData.streams.map((stream) => {
                  return {
                    streamName: stream.streamName,
                    streamNamespace: stream.streamNamespace,
                    configType: stream.configType,
                  };
                }),
                configType: syncProgressData.configType,
                jobId: syncProgressData.jobId,
              },
              user: { email: "", name: "", id: "" },
            },
          ]
        : []), // if there is a running sync, append an item to the top of the list
      ...(connectionEventsData?.pages.flatMap<ConnectionEvent>((page) => page.data.events) ?? []),
    ];

    return events;
  }, [connection.connectionId, connectionEventsData?.pages, showRunningJob, syncProgressData]);

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

  if (connectionEventsToShow.length === 0) {
    return (
      <Box p="xl">
        <EmptyState text={<FormattedMessage id="connection.timeline.empty" />} />
      </Box>
    );
  }

  return (
    <>
      <Virtuoso
        data={connectionEventsToShow}
        customScrollParent={scrollElement ?? undefined}
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
          return <EventLineItem event={event} key={event.id} />;
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
