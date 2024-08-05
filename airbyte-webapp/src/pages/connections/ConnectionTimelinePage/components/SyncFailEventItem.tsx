import { FormattedMessage, useIntl } from "react-intl";
import { InferType } from "yup";

import { Box } from "components/ui/Box";
import { Text } from "components/ui/Text";

import { JobFailureDetails } from "area/connection/components/JobHistoryItem/JobFailureDetails";
import { failureUiDetailsFromReason } from "core/utils/errorStatusMessage";

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
  const { formatMessage } = useIntl();
  const titleId = titleIdMap[syncEvent.eventType];

  const failureUiDetails = failureUiDetailsFromReason(syncEvent.summary.failureReason, formatMessage);
  const jobStatus = getStatusByEventType(syncEvent.eventType);

  if (!failureUiDetails) {
    return null;
  }

  return (
    <ConnectionTimelineEventItem>
      <ConnectionTimelineEventIcon icon="sync" statusIcon={getStatusIcon(jobStatus)} />
      <ConnectionTimelineEventSummary>
        <Text bold>
          <FormattedMessage id={titleId} />
        </Text>
        <Box pt="xs" className={styles.details}>
          <JobFailureDetails failureUiDetails={failureUiDetails} />
        </Box>
      </ConnectionTimelineEventSummary>
      <ConnectionTimelineEventActions
        createdAt={syncEvent.createdAt}
        jobId={syncEvent.summary.jobId}
        eventId={syncEvent.id}
      />
    </ConnectionTimelineEventItem>
  );
};
