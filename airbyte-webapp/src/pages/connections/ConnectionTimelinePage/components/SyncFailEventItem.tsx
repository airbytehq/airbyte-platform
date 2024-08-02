import { FormattedDate, FormattedMessage, useIntl } from "react-intl";
import { InferType } from "yup";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { JobFailureDetails } from "area/connection/components/JobHistoryItem/JobFailureDetails";
import { failureUiDetailsFromReason } from "core/utils/errorStatusMessage";

import styles from "./SyncFailEventItem.module.scss";
import { ConnectionTimelineEventIcon } from "../ConnectionTimelineEventIcon";
import { ConnectionTimelineEventItem } from "../ConnectionTimelineEventItem";
import { JobEventMenu } from "../JobEventMenu";
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
      <div className={styles.container}>
        <Text bold>
          <FormattedMessage id={titleId} />
        </Text>
        <Box pt="xs" className={styles.failureDetails}>
          <JobFailureDetails failureUiDetails={failureUiDetails} />
        </Box>
      </div>
      <FlexContainer direction="row" gap="xs" alignItems="center" className={styles.endContent}>
        <Text color="grey400">
          <FormattedDate value={syncEvent.createdAt * 1000} timeStyle="short" dateStyle="medium" />
        </Text>
        <JobEventMenu eventId={syncEvent.id} jobId={syncEvent.summary.jobId} />
      </FlexContainer>
    </ConnectionTimelineEventItem>
  );
};
