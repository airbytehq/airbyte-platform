import { FormattedMessage } from "react-intl";
import { InferType } from "yup";

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
  clearEvent: InferType<typeof clearEventSchema>;
}
export const ClearEventItem: React.FC<ClearEventProps> = ({ clearEvent }) => {
  const [showExtendedStats] = useLocalStorage("airbyte_extended-attempts-stats", false);
  const title = titleIdMap[clearEvent.eventType];
  const jobStatus = getStatusByEventType(clearEvent.eventType);
  const streamsToList = clearEvent.summary.streams.map((stream) => stream.name);

  return (
    <ConnectionTimelineEventItem>
      <ConnectionTimelineEventIcon icon="cross" statusIcon={getStatusIcon(jobStatus)} />
      <ConnectionTimelineEventSummary>
        <Text bold>
          <FormattedMessage id={title} values={{ value: streamsToList.length }} />
        </Text>
        <Box pt="xs">
          {jobStatus === "cancelled" && !!clearEvent.user && (
            <div>
              <UserCancelledDescription user={clearEvent.user} jobType="clear" />
            </div>
          )}
          {streamsToList.length > 0 && <ResetStreamsDetails names={streamsToList} />}
          {showExtendedStats && (
            <>
              <Text as="span" color="grey400" size="sm">
                <FormattedMessage id="jobs.jobId" values={{ id: clearEvent.summary.jobId }} />
              </Text>
              <Text as="span" color="grey400" size="sm">
                |
              </Text>
              <Text as="span" color="grey400" size="sm">
                <FormattedMessage id="jobs.attemptCount" values={{ count: clearEvent.summary.attemptsCount }} />
              </Text>
            </>
          )}
        </Box>
      </ConnectionTimelineEventSummary>
      <ConnectionTimelineEventActions
        createdAt={clearEvent.createdAt}
        eventId={clearEvent.id}
        jobId={clearEvent.summary.jobId}
        attemptCount={clearEvent.summary.attemptsCount}
      />
    </ConnectionTimelineEventItem>
  );
};
