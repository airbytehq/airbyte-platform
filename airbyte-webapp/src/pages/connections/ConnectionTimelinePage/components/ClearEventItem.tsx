import { FormattedMessage } from "react-intl";
import { z } from "zod";

import { Box } from "components/ui/Box";
import { Text } from "components/ui/Text";

import { ResetStreamsDetails } from "area/connection/components/JobHistoryItem/ResetStreamDetails";
import { useLocalStorage } from "core/utils/useLocalStorage";

import { UserCancelledDescription } from "./TimelineEventUser";
import { ConnectionTimelineEventActions } from "../ConnectionTimelineEventActions";
import { ConnectionTimelineEventIcon } from "../ConnectionTimelineEventIcon";
import { ConnectionTimelineEventItem } from "../ConnectionTimelineEventItem";
import { ConnectionTimelineEventSummary } from "../ConnectionTimelineEventSummary";
import { clearEventSchema } from "../types";
import { getStatusByEventType, getStatusIcon, titleIdMap } from "../utils";

interface ClearEventProps {
  event: z.infer<typeof clearEventSchema>;
}
export const ClearEventItem: React.FC<ClearEventProps> = ({ event }) => {
  const [showExtendedStats] = useLocalStorage("airbyte_extended-attempts-stats", false);
  const title = titleIdMap[event.eventType];
  const jobStatus = getStatusByEventType(event.eventType);
  const streamsToList = event.summary.streams.map((stream) => stream.name);

  return (
    <ConnectionTimelineEventItem>
      <ConnectionTimelineEventIcon icon="cross" statusIcon={getStatusIcon(jobStatus)} />
      <ConnectionTimelineEventSummary>
        <Text bold>
          <FormattedMessage id={title} values={{ value: streamsToList.length }} />
        </Text>
        <Box pt="xs">
          {jobStatus === "cancelled" && !!event.user && (
            <div>
              <UserCancelledDescription user={event.user} jobType="clear" />
            </div>
          )}
          {streamsToList.length > 0 && <ResetStreamsDetails names={streamsToList} />}
          {showExtendedStats && (
            <>
              <Text as="span" color="grey400" size="sm">
                <FormattedMessage id="jobs.jobId" values={{ id: event.summary.jobId }} />
              </Text>
              <Text as="span" color="grey400" size="sm">
                |
              </Text>
              <Text as="span" color="grey400" size="sm">
                <FormattedMessage id="jobs.attemptCount" values={{ count: event.summary.attemptsCount }} />
              </Text>
            </>
          )}
        </Box>
      </ConnectionTimelineEventSummary>
      <ConnectionTimelineEventActions
        createdAt={event.createdAt}
        eventId={event.id}
        jobId={event.summary.jobId}
        attemptCount={event.summary.attemptsCount}
      />
    </ConnectionTimelineEventItem>
  );
};
