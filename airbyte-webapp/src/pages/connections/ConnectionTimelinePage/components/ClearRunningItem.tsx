import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Text } from "components/ui/Text";

import { ResetStreamsDetails } from "area/connection/components/JobHistoryItem/ResetStreamDetails";

import { ConnectionTimelineEventActions } from "../ConnectionTimelineEventActions";
import { ConnectionTimelineEventIcon } from "../ConnectionTimelineEventIcon";
import { ConnectionTimelineEventItem } from "../ConnectionTimelineEventItem";
import { ConnectionTimelineEventSummary } from "../ConnectionTimelineEventSummary";

interface ClearRunningItemProps {
  jobId: number;
  streams: string[];
  startedAt: number;
}

export const ClearRunningItem: React.FC<ClearRunningItemProps> = ({ jobId, streams, startedAt }) => {
  return (
    <ConnectionTimelineEventItem centered>
      <ConnectionTimelineEventIcon icon="cross" running />
      <ConnectionTimelineEventSummary>
        <Text bold>
          <FormattedMessage id="connection.timeline.clear_running" values={{ count: streams.length }} />
        </Text>

        <Box pt="xs">{streams.length > 0 && <ResetStreamsDetails names={streams} />}</Box>
      </ConnectionTimelineEventSummary>
      <ConnectionTimelineEventActions createdAt={startedAt} jobId={jobId} />
    </ConnectionTimelineEventItem>
  );
};
