import { FormattedDate, FormattedMessage } from "react-intl";

import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { ConnectionTimelineEventIcon } from "../ConnectionTimelineEventIcon";
import { ConnectionTimelineEventItem } from "../ConnectionTimelineEventItem";
import { JobEventMenu } from "../JobEventMenu";

interface SyncRunningItemProps {
  startedAt: number;
  jobId: number;
}

export const SyncRunningItem: React.FC<SyncRunningItemProps> = ({ startedAt, jobId }) => {
  return (
    <ConnectionTimelineEventItem centered>
      <ConnectionTimelineEventIcon icon="sync" running />
      <FlexItem grow>
        <Text bold>
          <FormattedMessage id="connection.timeline.sync_running" />
        </Text>
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
