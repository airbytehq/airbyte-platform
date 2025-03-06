import { FormattedMessage, useIntl } from "react-intl";
import { InferType } from "yup";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { JobFailureDetails } from "area/connection/components/JobHistoryItem/JobFailureDetails";
import { ResetStreamsDetails } from "area/connection/components/JobHistoryItem/ResetStreamDetails";
import { failureUiDetailsFromReason } from "core/utils/errorStatusMessage";

import { JobStats } from "./JobStats";
import { UserCancelledDescription } from "./TimelineEventUser";
import { ConnectionTimelineEventActions } from "../ConnectionTimelineEventActions";
import { ConnectionTimelineEventIcon } from "../ConnectionTimelineEventIcon";
import { ConnectionTimelineEventItem } from "../ConnectionTimelineEventItem";
import { ConnectionTimelineEventSummary } from "../ConnectionTimelineEventSummary";
import { refreshEventSchema } from "../types";
import { getStatusByEventType, getStatusIcon, titleIdMap } from "../utils";

interface RefreshEventItemProps {
  event: InferType<typeof refreshEventSchema>;
}
export const RefreshEventItem: React.FC<RefreshEventItemProps> = ({ event }) => {
  const titleId = titleIdMap[event.eventType];
  const jobStatus = getStatusByEventType(event.eventType);
  const streamsToList = event.summary.streams.map((stream) => stream.name);
  const { formatMessage } = useIntl();
  const failureUiDetails = !!event.summary.failureReason
    ? failureUiDetailsFromReason(event.summary.failureReason, formatMessage)
    : undefined;

  return (
    <ConnectionTimelineEventItem>
      <ConnectionTimelineEventIcon icon="rotate" statusIcon={getStatusIcon(jobStatus)} />

      <ConnectionTimelineEventSummary>
        <FlexContainer gap="xs" direction="column">
          <Text bold>
            <FormattedMessage id={titleId} values={{ value: streamsToList.length }} />
          </Text>
          <FlexContainer gap="xs" alignItems="baseline">
            {jobStatus === "cancelled" && !!event.user && (
              <UserCancelledDescription user={event.user} jobType="refresh" />
            )}

            <JobStats {...event.summary} />
          </FlexContainer>
          {failureUiDetails && (
            <Box pt="xs">
              <JobFailureDetails failureUiDetails={failureUiDetails} />
            </Box>
          )}
          {streamsToList.length > 0 && <ResetStreamsDetails names={streamsToList} />}
        </FlexContainer>
      </ConnectionTimelineEventSummary>
      <ConnectionTimelineEventActions createdAt={event.createdAt} eventId={event.id} jobId={event.summary.jobId} />
    </ConnectionTimelineEventItem>
  );
};
