import { FormattedMessage, useIntl } from "react-intl";
import { InferType } from "yup";

import { Text } from "components/ui/Text";

import { JobFailureDetails } from "area/connection/components/JobHistoryItem/JobFailureDetails";
import { failureUiDetailsFromReason } from "core/utils/errorStatusMessage";
import { useLocalStorage } from "core/utils/useLocalStorage";

import { JobStats } from "./JobStats";
import { ConnectionTimelineEventActions } from "../ConnectionTimelineEventActions";
import { ConnectionTimelineEventIcon } from "../ConnectionTimelineEventIcon";
import { ConnectionTimelineEventItem } from "../ConnectionTimelineEventItem";
import { ConnectionTimelineEventSummary } from "../ConnectionTimelineEventSummary";
import { syncFailEventSchema } from "../types";
import { getStatusByEventType, getStatusIcon, titleIdMap } from "../utils";

interface SyncFailEventItemProps {
  event: InferType<typeof syncFailEventSchema>;
}

export const SyncFailEventItem: React.FC<SyncFailEventItemProps> = ({ event }) => {
  const [showExtendedStats] = useLocalStorage("airbyte_extended-attempts-stats", false);

  const { formatMessage } = useIntl();
  const titleId = titleIdMap[event.eventType];

  const failureUiDetails = failureUiDetailsFromReason(event.summary.failureReason, formatMessage);
  const jobStatus = getStatusByEventType(event.eventType);

  return (
    <ConnectionTimelineEventItem>
      <ConnectionTimelineEventIcon icon="sync" statusIcon={getStatusIcon(jobStatus)} />
      <ConnectionTimelineEventSummary>
        <Text bold>
          <FormattedMessage id={titleId} />
        </Text>
        <JobStats {...event.summary} />
        {failureUiDetails && <JobFailureDetails failureUiDetails={failureUiDetails} />}
        {!failureUiDetails && showExtendedStats && (
          <Text as="span" color="grey400" size="sm">
            <FormattedMessage id="jobs.jobId" values={{ id: event.summary.jobId }} />
          </Text>
        )}
      </ConnectionTimelineEventSummary>
      <ConnectionTimelineEventActions createdAt={event.createdAt} jobId={event.summary.jobId} eventId={event.id} />
    </ConnectionTimelineEventItem>
  );
};
