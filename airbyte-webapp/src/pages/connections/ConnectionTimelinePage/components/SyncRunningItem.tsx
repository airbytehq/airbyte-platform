import { FormattedMessage } from "react-intl";

import { Text } from "components/ui/Text";

import { ConnectionTimelineEventActions } from "../ConnectionTimelineEventActions";
import { ConnectionTimelineEventIcon } from "../ConnectionTimelineEventIcon";
import { ConnectionTimelineEventItem } from "../ConnectionTimelineEventItem";
import { ConnectionTimelineEventSummary } from "../ConnectionTimelineEventSummary";

interface SyncRunningItemProps {
  startedAt: number;
  jobId: number;
}

export const SyncRunningItem: React.FC<SyncRunningItemProps> = ({ startedAt, jobId }) => {
  return (
    <ConnectionTimelineEventItem centered>
      <ConnectionTimelineEventIcon icon="sync" running />
      <ConnectionTimelineEventSummary>
        <Text bold>
          <FormattedMessage id="connection.timeline.sync_running" />
        </Text>
      </ConnectionTimelineEventSummary>

      <ConnectionTimelineEventActions createdAt={startedAt} jobId={jobId} />
    </ConnectionTimelineEventItem>
  );
};
