import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexItem } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { ResetStreamsDetails } from "area/connection/components/JobHistoryItem/ResetStreamDetails";
import { useLocalStorage } from "core/utils/useLocalStorage";

import { ConnectionTimelineEventIcon } from "../ConnectionTimelineEventIcon";
import { ConnectionTimelineEventItem } from "../ConnectionTimelineEventItem";
import { JobEventMenu } from "../JobEventMenu";
import { ConnectionTimelineEventType, getStatusIcon, titleIdMap } from "../utils";

interface ClearEventProps {
  jobId: number;
  eventType: ConnectionTimelineEventType;
  eventId: string;
  clearedStreams: string[];
  attemptsCount: number;
  jobStatus: "incomplete" | "failed" | "succeeded" | "cancelled";
}
export const ClearEvent: React.FC<ClearEventProps> = ({
  jobId,
  eventType,
  eventId,
  attemptsCount,
  jobStatus,
  clearedStreams,
}) => {
  const [showExtendedStats] = useLocalStorage("airbyte_extended-attempts-stats", false);
  const title = titleIdMap[eventType];
  return (
    <ConnectionTimelineEventItem>
      <ConnectionTimelineEventIcon isLast={false} icon="sync" statusIcon={getStatusIcon(jobStatus)} />

      <FlexItem grow>
        <Text bold>
          <FormattedMessage id={title} values={{ value: clearedStreams.length }} />
        </Text>
        <Box pt="xs">
          <ResetStreamsDetails names={clearedStreams} />
          {showExtendedStats && (
            <>
              <Text as="span" color="grey500" size="sm">
                |
              </Text>
              <Text as="span" color="grey500" size="sm">
                <FormattedMessage id="jobs.jobId" values={{ id: jobId }} />
              </Text>
              <Text as="span" color="grey500" size="sm">
                |
              </Text>
              <Text as="span" color="grey500" size="sm">
                <FormattedMessage id="jobs.attemptCount" values={{ count: attemptsCount }} />
              </Text>
            </>
          )}
        </Box>
      </FlexItem>
      <JobEventMenu eventId={eventId} jobId={jobId} />
    </ConnectionTimelineEventItem>
  );
};
