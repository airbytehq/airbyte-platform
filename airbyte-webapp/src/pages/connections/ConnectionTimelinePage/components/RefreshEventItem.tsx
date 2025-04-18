import { FormattedMessage, useIntl } from "react-intl";
import { z } from "zod";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { AISyncFailureDrawerTitle, AISyncFailureExplanation } from "area/connection/components";
import { AISyncFailureExplanationButton } from "area/connection/components/AISyncFailureExplanationButton";
import { JobFailureDetails } from "area/connection/components/JobHistoryItem/JobFailureDetails";
import { ResetStreamsDetails } from "area/connection/components/JobHistoryItem/ResetStreamDetails";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { useDrawerActions } from "core/services/ui/DrawerService";
import { failureUiDetailsFromReason } from "core/utils/errorStatusMessage";
import { useExperiment } from "hooks/services/Experiment";

import { JobStats } from "./JobStats";
import { UserCancelledDescription } from "./TimelineEventUser";
import { ConnectionTimelineEventActions } from "../ConnectionTimelineEventActions";
import { ConnectionTimelineEventIcon } from "../ConnectionTimelineEventIcon";
import { ConnectionTimelineEventItem } from "../ConnectionTimelineEventItem";
import { ConnectionTimelineEventSummary } from "../ConnectionTimelineEventSummary";
import { refreshEventSchema } from "../types";
import { getStatusByEventType, getStatusIcon, titleIdMap } from "../utils";

interface RefreshEventItemProps {
  event: z.infer<typeof refreshEventSchema>;
}
export const RefreshEventItem: React.FC<RefreshEventItemProps> = ({ event }) => {
  const titleId = titleIdMap[event.eventType];
  const jobStatus = getStatusByEventType(event.eventType);
  const streamsToList = event.summary.streams.map((stream) => stream.name);
  const { formatMessage } = useIntl();
  const failureUiDetails = !!event.summary.failureReason
    ? failureUiDetailsFromReason(event.summary.failureReason, formatMessage)
    : undefined;
  const llmSyncFailureExperimentEnabled = useExperiment("platform.llm-sync-job-failure-explanation");
  const { openDrawer } = useDrawerActions();
  const analyticsService = useAnalyticsService();

  const showAIJobExplanation = () => {
    if (!llmSyncFailureExperimentEnabled) {
      return;
    }
    analyticsService.track(Namespace.CONNECTIONS, Action.REFRESH_FAILURE_EXPLANATION_OPENED, {
      jobId: event.summary.jobId,
    });
    openDrawer({
      title: <AISyncFailureDrawerTitle />,
      content: (
        <Box px="lg">
          <AISyncFailureExplanation
            jobSummary={event.summary}
            jobStatus={jobStatus}
            titleId={titleId}
            titleValues={{ value: streamsToList.length }}
          />
        </Box>
      ),
    });
  };

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
            <JobStats summary={event.summary} />
          </FlexContainer>
          {failureUiDetails && (
            <Box pt="xs">
              <JobFailureDetails failureUiDetails={failureUiDetails} />
            </Box>
          )}
          {llmSyncFailureExperimentEnabled && failureUiDetails && (
            <Box my="md">
              <AISyncFailureExplanationButton onClick={showAIJobExplanation}>
                <FormattedMessage id="connection.llmSyncFailureExplanation.explain" />
              </AISyncFailureExplanationButton>
            </Box>
          )}
          {streamsToList.length > 0 && <ResetStreamsDetails names={streamsToList} />}
        </FlexContainer>
      </ConnectionTimelineEventSummary>
      <ConnectionTimelineEventActions createdAt={event.createdAt} eventId={event.id} jobId={event.summary.jobId} />
    </ConnectionTimelineEventItem>
  );
};
