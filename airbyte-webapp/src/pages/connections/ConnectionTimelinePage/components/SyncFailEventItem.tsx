import { FormattedMessage, useIntl } from "react-intl";
import { InferType } from "yup";

import { Box } from "components/ui/Box";
import { Text } from "components/ui/Text";

import { JobFailureDetails } from "area/connection/components/JobHistoryItem/JobFailureDetails";
import { failureUiDetailsFromReason } from "core/utils/errorStatusMessage";
import { useLocalStorage } from "core/utils/useLocalStorage";

import { JobStats } from "./JobStats";
import styles from "./SyncFailEventItem.module.scss";
import { ConnectionTimelineEventActions } from "../ConnectionTimelineEventActions";
import { ConnectionTimelineEventIcon } from "../ConnectionTimelineEventIcon";
import { ConnectionTimelineEventItem } from "../ConnectionTimelineEventItem";
import { ConnectionTimelineEventSummary } from "../ConnectionTimelineEventSummary";
import { syncFailEventSchema } from "../types";
import { getStatusByEventType, getStatusIcon, titleIdMap } from "../utils";

interface SyncFailEventItemProps {
  syncEvent: InferType<typeof syncFailEventSchema>;
}

export const SyncFailEventItem: React.FC<SyncFailEventItemProps> = ({ syncEvent }) => {
  const [showExtendedStats] = useLocalStorage("airbyte_extended-attempts-stats", false);

  const { formatMessage } = useIntl();
  const titleId = titleIdMap[syncEvent.eventType];

  const failureUiDetails = failureUiDetailsFromReason(syncEvent.summary.failureReason, formatMessage);
  const jobStatus = getStatusByEventType(syncEvent.eventType);

  return (
    <ConnectionTimelineEventItem>
      <ConnectionTimelineEventIcon icon="sync" statusIcon={getStatusIcon(jobStatus)} />
      <ConnectionTimelineEventSummary>
        <Text bold>
          <FormattedMessage id={titleId} />
        </Text>
        <JobStats {...syncEvent.summary} />
        {failureUiDetails && (
          <Box pt="xs" className={styles.details}>
            <JobFailureDetails failureUiDetails={failureUiDetails} />
          </Box>
        )}
        {!failureUiDetails && showExtendedStats && (
          <Text as="span" color="grey400" size="sm">
            <FormattedMessage id="jobs.jobId" values={{ id: syncEvent.summary.jobId }} />
          </Text>
        )}
      </ConnectionTimelineEventSummary>
      <ConnectionTimelineEventActions
        createdAt={syncEvent.createdAt}
        jobId={syncEvent.summary.jobId}
        eventId={syncEvent.id}
      />
    </ConnectionTimelineEventItem>
  );
};
