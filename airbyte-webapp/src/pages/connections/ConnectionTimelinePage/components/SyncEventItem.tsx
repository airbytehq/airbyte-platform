import { FormattedMessage } from "react-intl";
import { z } from "zod";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { JobStats } from "./JobStats";
import { UserCancelledDescription } from "./TimelineEventUser";
import { ConnectionTimelineEventActions } from "../ConnectionTimelineEventActions";
import { ConnectionTimelineEventIcon } from "../ConnectionTimelineEventIcon";
import { ConnectionTimelineEventItem } from "../ConnectionTimelineEventItem";
import { ConnectionTimelineEventSummary } from "../ConnectionTimelineEventSummary";
import { syncEventSchema } from "../types";
import { getStatusByEventType, getStatusIcon, titleIdMap } from "../utils";

interface SyncEventProps {
  event: z.infer<typeof syncEventSchema>;
}

export const SyncEventItem: React.FC<SyncEventProps> = ({ event }) => {
  const titleId = titleIdMap[event.eventType];

  const jobStatus = getStatusByEventType(event.eventType);

  return (
    <ConnectionTimelineEventItem>
      <ConnectionTimelineEventIcon icon="sync" statusIcon={getStatusIcon(jobStatus)} />
      <ConnectionTimelineEventSummary>
        <Text bold>
          <FormattedMessage id={titleId} />
        </Text>
        <Box pt="xs">
          <FlexContainer gap="sm" alignItems="baseline">
            {jobStatus === "cancelled" && !!event.user && <UserCancelledDescription user={event.user} jobType="sync" />}
            <JobStats summary={event.summary} />
          </FlexContainer>
        </Box>
      </ConnectionTimelineEventSummary>
      <ConnectionTimelineEventActions createdAt={event.createdAt} eventId={event.id} jobId={event.summary.jobId} />
    </ConnectionTimelineEventItem>
  );
};
