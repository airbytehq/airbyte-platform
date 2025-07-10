import dayjs from "dayjs";
import { useMemo } from "react";
import { FormattedMessage, FormattedTime } from "react-intl";
// In this list we wrap an entire component in a link, so we do not want to use our internally styled Link component
// eslint-disable-next-line no-restricted-imports
import { Link } from "react-router-dom";

import { FormattedTimeRange } from "components/FormattedTimeRange";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { IconType } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { useCurrentWorkspaceLink } from "area/workspace/utils";
import { ConnectionEventMinimal } from "core/api/types/AirbyteClient";
import { ConnectionTimelineEventIcon } from "pages/connections/ConnectionTimelinePage/ConnectionTimelineEventIcon";
import { ConnectionRoutePaths, RoutePaths } from "pages/routePaths";

import styles from "./ConnectionEventsList.module.scss";
import { SyncCount } from "./SyncCount";

interface ConnectionEventsListProps {
  events: ConnectionEventMinimal[];
  start: Date;
  end: Date;
}

const syncStatusKeyMap: Record<
  "SYNC_FAILED" | "REFRESH_FAILED" | "SYNC_SUCCEEDED" | "REFRESH_SUCCEEDED" | "SYNC_INCOMPLETE" | "REFRESH_INCOMPLETE",
  { key: string; icon: IconType; statusIcon: IconType | undefined }
> = {
  SYNC_FAILED: { key: "jobs.jobStatus.sync.failed", icon: "connection", statusIcon: "statusError" },
  REFRESH_FAILED: { key: "jobs.jobStatus.sync.failed", icon: "connection", statusIcon: "statusError" },
  SYNC_SUCCEEDED: { key: "jobs.jobStatus.sync.succeeded", icon: "connection", statusIcon: "statusSuccess" },
  REFRESH_SUCCEEDED: { key: "jobs.jobStatus.sync.succeeded", icon: "connection", statusIcon: "statusSuccess" },
  SYNC_INCOMPLETE: { key: "jobs.jobStatus.sync.partialSuccess", icon: "connection", statusIcon: "statusWarning" },
  REFRESH_INCOMPLETE: { key: "jobs.jobStatus.sync.partialSuccess", icon: "connection", statusIcon: "statusWarning" },
};

function eventTypeInKeyMap(eventType: ConnectionEventMinimal["eventType"]): eventType is keyof typeof syncStatusKeyMap {
  return eventType in syncStatusKeyMap;
}

export const ConnectionEventsList: React.FC<ConnectionEventsListProps> = ({ start, end, events }) => {
  const createLink = useCurrentWorkspaceLink();
  const failedCount = useMemo(() => {
    return events.filter((event) => event.eventType === "SYNC_FAILED" || event.eventType === "REFRESH_FAILED").length;
  }, [events]);
  const successCount = useMemo(() => {
    return events.filter((event) => event.eventType === "SYNC_SUCCEEDED" || event.eventType === "REFRESH_SUCCEEDED")
      .length;
  }, [events]);
  const partialSuccessCount = useMemo(() => {
    return events.filter((event) => event.eventType === "REFRESH_INCOMPLETE" || event.eventType === "SYNC_INCOMPLETE")
      .length;
  }, [events]);

  const connectionsWithEvents = useMemo(() => {
    const unsortedConnections = events.reduce((acc, event) => {
      if (!acc.has(event.connectionId)) {
        acc.set(event.connectionId, { connectionName: event.connectionName, events: [] });
      }
      acc.get(event.connectionId)?.events.push(event);

      return acc;
    }, new Map<string, { connectionName: string; events: ConnectionEventMinimal[] }>());

    const alphabeticallySortedGroups = new Map(
      Array.from(unsortedConnections.entries()).sort(
        ([, { connectionName: connectionNameA }], [, { connectionName: connectionNameB }]) => {
          return connectionNameA.localeCompare(connectionNameB);
        }
      )
    );

    // Sort events in place by createdAt descending
    alphabeticallySortedGroups.forEach(({ events }) => {
      events.sort((eventA, eventB) => eventB.createdAt.localeCompare(eventA.createdAt));
    });

    return alphabeticallySortedGroups;
  }, [events]);

  const windowIsLessThanOneDay = Math.abs(dayjs(end).diff(dayjs(start), "day")) < 1;

  return (
    <Box px="lg">
      <FlexContainer direction="column">
        {windowIsLessThanOneDay && (
          <Text color="grey400" size="sm">
            <FormattedTimeRange from={start} to={end} />
          </Text>
        )}
        <SyncCount failedCount={failedCount} successCount={successCount} partialSuccessCount={partialSuccessCount} />
        {Array.from(connectionsWithEvents.entries()).map(([connectionId, { connectionName, events }]) => {
          return (
            <FlexContainer direction="column" gap="none" key={connectionId}>
              <Text size="md" as="h3" className={styles.connectionEventsList__connectionTitle}>
                {connectionName}
              </Text>
              {events.map((event) => {
                if (!eventTypeInKeyMap(event.eventType)) {
                  return null;
                }

                const linkTo = createLink(
                  `/${RoutePaths.Connections}/${event.connectionId}/${ConnectionRoutePaths.Timeline}?eventId=${event.eventId}`
                );
                return (
                  <Link className={styles.connectionEventsList__eventLink} to={linkTo} key={event.eventId}>
                    <ConnectionTimelineEventIcon
                      icon={syncStatusKeyMap[event.eventType].icon}
                      statusIcon={syncStatusKeyMap[event.eventType].statusIcon}
                    />
                    <FlexContainer direction="column" gap="xs">
                      <Text>
                        <FormattedMessage id={syncStatusKeyMap[event.eventType].key} />
                      </Text>
                      <Text color="grey400" size="sm">
                        <FormattedTime value={event.createdAt} />
                      </Text>
                    </FlexContainer>
                  </Link>
                );
              })}
            </FlexContainer>
          );
        })}
      </FlexContainer>
    </Box>
  );
};
