import { FormattedMessage } from "react-intl";
import { InferType } from "yup";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { formatBytes } from "core/utils/numberHelper";
import { useFormatDuration } from "core/utils/time";
import { useLocalStorage } from "core/utils/useLocalStorage";

import { ConnectionTimelineEventActions } from "../ConnectionTimelineEventActions";
import { ConnectionTimelineEventIcon } from "../ConnectionTimelineEventIcon";
import { ConnectionTimelineEventItem } from "../ConnectionTimelineEventItem";
import { ConnectionTimelineEventSummary } from "../ConnectionTimelineEventSummary";
import { syncEventSchema } from "../types";
import { getStatusByEventType, getStatusIcon, titleIdMap } from "../utils";

interface SyncEventProps {
  syncEvent: InferType<typeof syncEventSchema>;
}

export const SyncEventItem: React.FC<SyncEventProps> = ({ syncEvent }) => {
  const [showExtendedStats] = useLocalStorage("airbyte_extended-attempts-stats", false);
  const titleId = titleIdMap[syncEvent.eventType];
  const duration = useFormatDuration(
    (syncEvent.summary.startTimeEpochSeconds ?? 0) * 1000,
    (syncEvent.summary.endTimeEpochSeconds ?? 0) * 1000
  );

  const jobStatus = getStatusByEventType(syncEvent.eventType);

  const JobStats = () => {
    return (
      <FlexContainer gap="sm">
        <>
          <Text as="span" color="grey400" size="sm">
            {formatBytes(syncEvent.summary.bytesLoaded)}
          </Text>
          <Text as="span" color="grey400" size="sm">
            |
          </Text>
        </>

        {syncEvent.summary.recordsLoaded !== undefined && (
          <>
            <Text as="span" color="grey400" size="sm">
              <FormattedMessage id="sources.countRecordsLoaded" values={{ count: syncEvent.summary.recordsLoaded }} />
            </Text>
            <Text as="span" color="grey400" size="sm">
              |
            </Text>
          </>
        )}
        {!!syncEvent.summary.startTimeEpochSeconds && !!syncEvent.summary.startTimeEpochSeconds && (
          <Text as="span" color="grey400" size="sm">
            {duration}
          </Text>
        )}

        {showExtendedStats && (
          <>
            <Text as="span" color="grey400" size="sm">
              |
            </Text>
            <Text as="span" color="grey400" size="sm">
              <FormattedMessage id="jobs.jobId" values={{ id: syncEvent.summary.jobId }} />
            </Text>
            <Text as="span" color="grey400" size="sm">
              |
            </Text>
            <Text as="span" color="grey400" size="sm">
              <FormattedMessage id="jobs.attemptCount" values={{ count: syncEvent.summary.attemptsCount }} />
            </Text>
          </>
        )}
      </FlexContainer>
    );
  };

  return (
    <ConnectionTimelineEventItem>
      <ConnectionTimelineEventIcon icon="sync" statusIcon={getStatusIcon(jobStatus)} />
      <ConnectionTimelineEventSummary>
        <Text bold>
          <FormattedMessage id={titleId} />
        </Text>
        <Box pt="xs">
          <JobStats />
        </Box>
      </ConnectionTimelineEventSummary>
      <ConnectionTimelineEventActions
        createdAt={syncEvent.createdAt * 1000}
        eventId={syncEvent.id}
        jobId={syncEvent.summary.jobId}
      />
    </ConnectionTimelineEventItem>
  );
};
