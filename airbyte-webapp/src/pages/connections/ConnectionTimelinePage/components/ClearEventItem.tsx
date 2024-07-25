import { FormattedDate, FormattedMessage } from "react-intl";
import { InferType } from "yup";

import { Box } from "components/ui/Box";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { ResetStreamsDetails } from "area/connection/components/JobHistoryItem/ResetStreamDetails";
import { useLocalStorage } from "core/utils/useLocalStorage";

import { ConnectionTimelineEventIcon } from "../ConnectionTimelineEventIcon";
import { ConnectionTimelineEventItem } from "../ConnectionTimelineEventItem";
import { JobEventMenu } from "../JobEventMenu";
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
      <FlexItem grow>
        <Text bold>
          <FormattedMessage id={title} values={{ value: streamsToList.length }} />
        </Text>
        <Box pt="xs">
          {streamsToList.length > 0 && <ResetStreamsDetails names={streamsToList} />}
          {showExtendedStats && (
            <>
              <Text as="span" color="grey400" size="sm">
                |
              </Text>
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
      </FlexItem>
      <FlexContainer direction="row" gap="xs" alignItems="center">
        <Text color="grey400">
          <FormattedDate value={clearEvent.createdAt * 1000} timeStyle="short" dateStyle="medium" />
        </Text>
        <JobEventMenu eventId={clearEvent.id} jobId={clearEvent.summary.jobId} />
      </FlexContainer>
    </ConnectionTimelineEventItem>
  );
};
