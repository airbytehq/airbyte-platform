import { HTMLAttributes, Ref, forwardRef, useContext, useEffect, useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { Virtuoso } from "react-virtuoso";
import { InferType, SchemaOf } from "yup";

import { LoadingPage } from "components";
import { useConnectionStatus } from "components/connection/ConnectionStatus/useConnectionStatus";
import { EmptyState } from "components/EmptyState";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { LoadingSpinner } from "components/ui/LoadingSpinner";
import { ScrollParentContext } from "components/ui/ScrollParent";

import { useCurrentConnection, useGetConnectionSyncProgress, useListConnectionEventsInfinite } from "core/api";
import { ConnectionEvent, ConnectionSyncStatus } from "core/api/types/AirbyteClient";
import { trackError } from "core/utils/datadog";

import { ClearEventItem } from "./components/ClearEventItem";
import { ConnectionDisabledEventItem } from "./components/ConnectionDisabledEventItem";
import { ConnectionEnabledEventItem } from "./components/ConnectionEnabledEventItem";
import { ConnectionSettingsUpdateEventItem } from "./components/ConnectionSettingsUpdateEventItem";
import { DestinationConnectorUpdateEventItem } from "./components/DestinationConnectorUpdateEventItem";
import { JobStartEventItem } from "./components/JobStartEventItem";
import { MappingEventItem } from "./components/MappingEventItem";
import { RefreshEventItem } from "./components/RefreshEventItem";
import { RunningJobItem } from "./components/RunningJobItem";
import { SchemaUpdateEventItem } from "./components/SchemaUpdateEventItem";
import { SourceConnectorUpdateEventItem } from "./components/SourceConnectorUpdateEventItem";
import { SyncEventItem } from "./components/SyncEventItem";
import { SyncFailEventItem } from "./components/SyncFailEventItem";
import styles from "./ConnectionTimelineAllEventsList.module.scss";
import {
  ConnectionTimelineRunningEvent,
  clearEventSchema,
  connectionDisabledEventSchema,
  connectionEnabledEventSchema,
  connectionSettingsUpdateEventSchema,
  destinationConnectorUpdateEventSchema,
  generalEventSchema,
  jobRunningSchema,
  jobStartedEventSchema,
  mappingEventSchema,
  refreshEventSchema,
  schemaUpdateEventSchema,
  sourceConnectorUpdateEventSchema,
  syncEventSchema,
  syncFailEventSchema,
} from "./types";
import { eventTypeByStatusFilterValue, TimelineFilterValues, eventTypeByTypeFilterValue } from "./utils";

type AllSchemaEventTypes =
  | InferType<typeof jobRunningSchema>
  | InferType<typeof syncEventSchema>
  | InferType<typeof syncFailEventSchema>
  | InferType<typeof refreshEventSchema>
  | InferType<typeof clearEventSchema>
  | InferType<typeof jobStartedEventSchema>
  | InferType<typeof connectionEnabledEventSchema>
  | InferType<typeof connectionDisabledEventSchema>
  | InferType<typeof connectionSettingsUpdateEventSchema>
  | InferType<typeof schemaUpdateEventSchema>
  | InferType<typeof sourceConnectorUpdateEventSchema>
  | InferType<typeof destinationConnectorUpdateEventSchema>
  | InferType<typeof schemaUpdateEventSchema>
  | InferType<typeof mappingEventSchema>;

interface EventSchemaComponentMapItem<T> {
  schema: SchemaOf<T>;
  component: React.FC<{ event: T }>;
}

const eventSchemaComponentMap = [
  { schema: jobRunningSchema, component: RunningJobItem },
  { schema: syncEventSchema, component: SyncEventItem },
  { schema: syncFailEventSchema, component: SyncFailEventItem },
  { schema: refreshEventSchema, component: RefreshEventItem },
  { schema: clearEventSchema, component: ClearEventItem },
  { schema: jobStartedEventSchema, component: JobStartEventItem },
  { schema: connectionEnabledEventSchema, component: ConnectionEnabledEventItem },
  { schema: connectionDisabledEventSchema, component: ConnectionDisabledEventItem },
  { schema: connectionSettingsUpdateEventSchema, component: ConnectionSettingsUpdateEventItem },
  { schema: schemaUpdateEventSchema, component: SchemaUpdateEventItem },
  { schema: sourceConnectorUpdateEventSchema, component: SourceConnectorUpdateEventItem },
  { schema: destinationConnectorUpdateEventSchema, component: DestinationConnectorUpdateEventItem },
  { schema: mappingEventSchema, component: MappingEventItem },
] as Array<EventSchemaComponentMapItem<AllSchemaEventTypes>>;

export const validateAndMapEvent = (event: ConnectionEvent | ConnectionTimelineRunningEvent) => {
  for (const { schema, component: Component } of eventSchemaComponentMap) {
    if (schema.isValidSync(event, { recursive: true, stripUnknown: true })) {
      return (
        <Box py="lg" key={event.id}>
          <Component event={event} />
        </Box>
      );
    }
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
