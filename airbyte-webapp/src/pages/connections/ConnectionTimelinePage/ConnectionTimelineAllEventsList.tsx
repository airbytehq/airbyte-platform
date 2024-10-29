import { HTMLAttributes, Ref, forwardRef, useEffect, useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { Virtuoso } from "react-virtuoso";
import { InferType } from "yup";

import { LoadingPage } from "components";
import { useConnectionStatus } from "components/connection/ConnectionStatus/useConnectionStatus";
import { EmptyState } from "components/EmptyState";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { LoadingSpinner } from "components/ui/LoadingSpinner";

import { useCurrentConnection, useGetConnectionSyncProgress, useListConnectionEventsInfinite } from "core/api";
import { ConnectionEvent, ConnectionSyncStatus } from "core/api/types/AirbyteClient";
import { trackError } from "core/utils/datadog";

import { ClearEventItem } from "./components/ClearEventItem";
import { ConnectionDisabledEventItem } from "./components/ConnectionDisabledEventItem";
import { ConnectionEnabledEventItem } from "./components/ConnectionEnabledEventItem";
import { ConnectionSettingsUpdateEventItem } from "./components/ConnectionSettingsUpdateEventItem";
import { JobStartEventItem } from "./components/JobStartEventItem";
import { RefreshEventItem } from "./components/RefreshEventItem";
import { RunningJobItem } from "./components/RunningJobItem";
import { SchemaUpdateEventItem } from "./components/SchemaUpdateEventItem";
import { SyncEventItem } from "./components/SyncEventItem";
import { SyncFailEventItem } from "./components/SyncFailEventItem";
import styles from "./ConnectionTimelineAllEventsList.module.scss";
import {
  ConnectionTimelineRunningEvent,
  clearEventSchema,
  connectionDisabledEventSchema,
  connectionEnabledEventSchema,
  connectionSettingsUpdateEventSchema,
  generalEventSchema,
  jobRunningSchema,
  jobStartedEventSchema,
  refreshEventSchema,
  schemaUpdateEventSchema,
  syncEventSchema,
  syncFailEventSchema,
} from "./types";
import { eventTypeByStatusFilterValue, TimelineFilterValues, eventTypeByTypeFilterValue } from "./utils";

export const validateAndMapEvent = (event: ConnectionEvent | ConnectionTimelineRunningEvent) => {
  if (jobRunningSchema.isValidSync(event, { recursive: true, stripUnknown: true })) {
    return (
      <Box py="lg" key={event.id}>
        <RunningJobItem jobRunningItem={event} />
      </Box>
    );
  } else if (syncEventSchema.isValidSync(event, { recursive: true, stripUnknown: true })) {
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
        <RefreshEventItem refreshEvent={event} />
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
  } else if (connectionEnabledEventSchema.isValidSync(event, { recursive: true, stripUnknown: true })) {
    return (
      <Box py="lg" key={event.id}>
        <ConnectionEnabledEventItem event={event} />
      </Box>
    );
  } else if (connectionDisabledEventSchema.isValidSync(event, { recursive: true, stripUnknown: true })) {
    return (
      <Box py="lg" key={event.id}>
        <ConnectionDisabledEventItem event={event} />
      </Box>
    );
  } else if (connectionSettingsUpdateEventSchema.isValidSync(event, { recursive: true, stripUnknown: true })) {
    return (
      <Box py="lg" key={event.id}>
        <ConnectionSettingsUpdateEventItem event={event} />
      </Box>
    );
  } else if (schemaUpdateEventSchema.isValidSync(event, { recursive: true, stripUnknown: true })) {
    return (
      <Box py="lg" key={event.id}>
        <SchemaUpdateEventItem event={event} />
      </Box>
    );
  }
  /**
   * known cases for excluding timeline events that we should not trigger error reporting for:
   * - events with only resourceRequirement patches
   * - events created prior to July 20 are not guaranteed to be valid
   */
  const isEventRecent = (createdAt: number | undefined): boolean => {
    const threshold = 1721433600;
    return !createdAt || createdAt > threshold;
  };

  const hasOnlyResourceRequirementPatches = (summary: InferType<typeof generalEventSchema>["summary"]): boolean => {
    return summary.patches && Object.keys(summary.patches).length === 1 && summary.patches.resourceRequirements;
  };

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

  const connectionEventsToShow: Array<ConnectionEvent | ConnectionTimelineRunningEvent> = useMemo(() => {
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
