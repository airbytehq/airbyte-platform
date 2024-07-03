import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { ConnectionTimelineJobStats } from "./ConnectionTimelineJobStats";
import {
  ConnectionTimelineEvent,
  ConnectionTimelineEventType,
  castEventSummaryToConnectionTimelineJobStatsProps,
} from "./utils";
export const ConnectionTimelineEventBody: React.FC<{
  eventType: ConnectionTimelineEvent["eventType"];
  eventSummary: ConnectionTimelineEvent["eventSummary"];
}> = ({ eventType, eventSummary }) => {
  const titleIdMap: Record<ConnectionTimelineEvent["eventType"], string> = {
    [ConnectionTimelineEventType.clear_cancelled]: "connection.timeline.clear_cancelled",
    [ConnectionTimelineEventType.clear_failed]: "connection.timeline.clear_failed",
    [ConnectionTimelineEventType.clear_incomplete]: "connection.timeline.clear_incomplete",
    [ConnectionTimelineEventType.clear_partially_succeeded]: "connection.timeline.clear_partially_succeeded",
    [ConnectionTimelineEventType.clear_succeeded]: "connection.timeline.clear_succeeded",
    [ConnectionTimelineEventType.refresh_cancelled]: "connection.timeline.refresh_cancelled",
    [ConnectionTimelineEventType.refresh_failed]: "connection.timeline.refresh_failed",
    [ConnectionTimelineEventType.refresh_incomplete]: "connection.timeline.refresh_incomplete",
    [ConnectionTimelineEventType.refresh_partially_succeeded]: "connection.timeline.refresh_partially_succeeded",
    [ConnectionTimelineEventType.refresh_succeeded]: "connection.timeline.refresh_succeeded",
    [ConnectionTimelineEventType.sync_cancelled]: "connection.timeline.sync_cancelled",
    [ConnectionTimelineEventType.sync_failed]: "connection.timeline.sync_failed",
    [ConnectionTimelineEventType.sync_incomplete]: "connection.timeline.sync_incomplete",
    [ConnectionTimelineEventType.sync_partially_succeeded]: "connection.timeline.sync_partially_succeeded",
    [ConnectionTimelineEventType.sync_succeeded]: "connection.timeline.sync_succeeded",
  };

  const streamCount = eventSummary.streams?.length || 0;

  const eventSummaryAsJobStats = castEventSummaryToConnectionTimelineJobStatsProps(eventSummary);

  return (
    <FlexContainer direction="column" gap="none">
      <Text bold>
        <FormattedMessage id={titleIdMap[eventType]} values={{ value: streamCount }} />
      </Text>
      {eventSummaryAsJobStats && (
        <ConnectionTimelineJobStats
          bytesCommitted={eventSummaryAsJobStats.bytesCommitted}
          recordsCommitted={eventSummaryAsJobStats.recordsCommitted}
          jobId={eventSummaryAsJobStats.jobId}
          jobStartedAt={eventSummaryAsJobStats.jobStartedAt}
          jobEndedAt={eventSummaryAsJobStats.jobEndedAt}
          attemptsCount={eventSummaryAsJobStats.attemptsCount}
          jobStatus={eventSummaryAsJobStats.jobStatus}
          failureSummary={eventSummaryAsJobStats.failureSummary}
        />
      )}
    </FlexContainer>
  );
};
