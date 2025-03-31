import { FormattedMessage, useIntl } from "react-intl";
import { z } from "zod";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { Text } from "components/ui/Text";

import { AISyncFailureDrawerTitle, AISyncFailureExplanation } from "area/connection/components";
import { JobFailureDetails } from "area/connection/components/JobHistoryItem/JobFailureDetails";
import { useDrawerActions } from "core/services/ui/DrawerService";
import { failureUiDetailsFromReason } from "core/utils/errorStatusMessage";
import { useLocalStorage } from "core/utils/useLocalStorage";
import { useExperiment } from "hooks/services/Experiment";

import { JobStats } from "./JobStats";
import { ConnectionTimelineEventActions } from "../ConnectionTimelineEventActions";
import { ConnectionTimelineEventIcon } from "../ConnectionTimelineEventIcon";
import { ConnectionTimelineEventItem } from "../ConnectionTimelineEventItem";
import { ConnectionTimelineEventSummary } from "../ConnectionTimelineEventSummary";
import { syncFailEventSchema } from "../types";
import { getStatusByEventType, getStatusIcon, titleIdMap } from "../utils";

interface SyncFailEventItemProps {
  event: z.infer<typeof syncFailEventSchema>;
}

export const SyncFailEventItem: React.FC<SyncFailEventItemProps> = ({ event }) => {
  const { openDrawer } = useDrawerActions();
  const [showExtendedStats] = useLocalStorage("airbyte_extended-attempts-stats", false);
  const { formatMessage } = useIntl();
  const titleId = titleIdMap[event.eventType];
  const llmSyncFailureExperimentEnabled = useExperiment("platform.llm-sync-job-failure-explanation");

  const failureUiDetails = failureUiDetailsFromReason(event.summary.failureReason, formatMessage);
  const jobStatus = getStatusByEventType(event.eventType);

  const showAIJobExplanation = () => {
    if (!llmSyncFailureExperimentEnabled) {
      return;
    }
    openDrawer({
      title: <AISyncFailureDrawerTitle />,
      content: (
        <Box px="lg">
          <AISyncFailureExplanation jobSummary={event.summary} jobStatus={jobStatus} titleId={titleId} />
        </Box>
      ),
    });
  };

  return (
    <ConnectionTimelineEventItem>
      <ConnectionTimelineEventIcon icon="sync" statusIcon={getStatusIcon(jobStatus)} />
      <ConnectionTimelineEventSummary>
        <Text bold>
          <FormattedMessage id={titleId} />
        </Text>
        <JobStats summary={event.summary} />
        {failureUiDetails && <JobFailureDetails failureUiDetails={failureUiDetails} />}
        {!failureUiDetails && showExtendedStats && (
          <Text as="span" color="grey400" size="sm">
            <FormattedMessage id="jobs.jobId" values={{ id: event.summary.jobId }} />
          </Text>
        )}
        {llmSyncFailureExperimentEnabled && (
          <Box mt="md">
            <Button onClick={showAIJobExplanation} variant="magic">
              <FormattedMessage id="connection.llmSyncFailureExplanation.explain" />
            </Button>
          </Box>
        )}
      </ConnectionTimelineEventSummary>
      <ConnectionTimelineEventActions createdAt={event.createdAt} jobId={event.summary.jobId} eventId={event.id} />
    </ConnectionTimelineEventItem>
  );
};
