import { FormattedMessage } from "react-intl";

import { Text } from "components/ui/Text";

import { useLocalStorage } from "core/utils/useLocalStorage";

import { ConnectionTimelineEventActions } from "../ConnectionTimelineEventActions";
import { ConnectionTimelineEventIcon } from "../ConnectionTimelineEventIcon";
import { ConnectionTimelineEventItem } from "../ConnectionTimelineEventItem";
import { ConnectionTimelineEventSummary } from "../ConnectionTimelineEventSummary";

interface SyncRunningItemProps {
  startedAt: number;
  jobId: number;
}

export const SyncRunningItem: React.FC<SyncRunningItemProps> = ({ startedAt, jobId }) => {
  const [showExtendedStats] = useLocalStorage("airbyte_extended-attempts-stats", false);

  return (
    <ConnectionTimelineEventItem>
      <ConnectionTimelineEventIcon icon="sync" running />
      <ConnectionTimelineEventSummary>
        <Text bold>
          <FormattedMessage id="connection.timeline.sync_running" />
        </Text>
        {showExtendedStats && (
          <Text as="span" color="grey400" size="sm">
            <FormattedMessage id="jobs.jobId" values={{ id: jobId }} />
          </Text>
        )}
      </ConnectionTimelineEventSummary>

      <ConnectionTimelineEventActions createdAt={startedAt} jobId={jobId} />
    </ConnectionTimelineEventItem>
  );
};
