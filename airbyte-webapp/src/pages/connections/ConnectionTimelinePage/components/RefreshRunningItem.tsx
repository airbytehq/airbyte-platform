import { FormattedDate, FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { ResetStreamsDetails } from "area/connection/components/JobHistoryItem/ResetStreamDetails";

import { ConnectionTimelineEventIcon } from "../ConnectionTimelineEventIcon";
import { ConnectionTimelineEventItem } from "../ConnectionTimelineEventItem";
import { JobEventMenu } from "../JobEventMenu";

interface RefreshRunningItemProps {
  jobId: number;
  streams: string[];
  startedAt: number;
}

export const RefreshRunningItem: React.FC<RefreshRunningItemProps> = ({ jobId, streams, startedAt }) => {
  return (
    <ConnectionTimelineEventItem centered>
      <ConnectionTimelineEventIcon icon="rotate" running />
      <FlexItem grow>
        <Text bold>
          <FormattedMessage id="connection.timeline.refresh_running" values={{ count: streams.length }} />
        </Text>
        <Box pt="xs">{streams.length > 0 && <ResetStreamsDetails names={streams} />}</Box>
      </FlexItem>

      <FlexContainer direction="row" gap="xs" alignItems="center">
        <Text color="grey400">
          <FormattedDate value={startedAt * 1000} timeStyle="short" dateStyle="medium" />
        </Text>
        <JobEventMenu jobId={jobId} />
      </FlexContainer>
    </ConnectionTimelineEventItem>
  );
};
