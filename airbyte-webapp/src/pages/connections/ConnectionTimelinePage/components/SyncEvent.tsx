import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { formatBytes } from "core/utils/numberHelper";
import { useFormatDuration } from "core/utils/time";
import { useLocalStorage } from "core/utils/useLocalStorage";

import { ConnectionTimelineEventIcon } from "../ConnectionTimelineEventIcon";
import { ConnectionTimelineEventItem } from "../ConnectionTimelineEventItem";
import { JobEventMenu } from "../JobEventMenu";
import { ConnectionTimelineEventType, getStatusIcon, titleIdMap } from "../utils";

interface SyncEventProps {
  bytesCommitted?: number;
  recordsCommitted?: number;
  jobId: number;
  failureMessage?: string;
  eventType: ConnectionTimelineEventType;
  eventId: string;
  jobStartedAt: number;
  jobEndedAt: number;
  attemptsCount: number;
  jobStatus: "incomplete" | "failed" | "succeeded" | "cancelled";
}

export const SyncEvent: React.FC<SyncEventProps> = ({
  bytesCommitted,
  recordsCommitted,
  jobId,
  eventId,
  failureMessage,
  eventType,
  jobStartedAt,
  jobEndedAt,
  attemptsCount,
  jobStatus,
}) => {
  const [showExtendedStats] = useLocalStorage("airbyte_extended-attempts-stats", false);
  const titleId = titleIdMap[eventType];
  const duration = useFormatDuration(jobStartedAt, jobEndedAt);

  const JobStats = () => {
    return (
      <FlexContainer gap="sm">
        <Text as="span" color="grey400" size="sm">
          {formatBytes(bytesCommitted)}
        </Text>
        <Text as="span" color="grey400" size="sm">
          |
        </Text>
        <Text as="span" color="grey400" size="sm">
          <FormattedMessage id="sources.countRecordsLoaded" values={{ count: recordsCommitted }} />
        </Text>
        <Text as="span" color="grey400" size="sm">
          |
        </Text>
        <Text as="span" color="grey400" size="sm">
          {duration}
        </Text>

        {showExtendedStats && (
          <>
            <Text as="span" color="grey400" size="sm">
              |
            </Text>
            <Text as="span" color="grey400" size="sm">
              <FormattedMessage id="jobs.jobId" values={{ id: jobId }} />
            </Text>
            <Text as="span" color="grey400" size="sm">
              |
            </Text>
            <Text as="span" color="grey400" size="sm">
              <FormattedMessage id="jobs.attemptCount" values={{ count: attemptsCount }} />
            </Text>
          </>
        )}
      </FlexContainer>
    );
  };

  return (
    <ConnectionTimelineEventItem>
      <ConnectionTimelineEventIcon isLast={false} icon="sync" statusIcon={getStatusIcon(jobStatus)} />
      <FlexItem grow>
        <Text bold>
          <FormattedMessage id={titleId} />
        </Text>
        <Box pt="xs">{jobStatus !== "failed" && <JobStats />}</Box>
        <Box pt="xs">
          {jobStatus === "failed" && (
            <FlexContainer gap="xs">
              <Text color="red" size="sm" as="span" bold>
                <FormattedMessage id="connection.timeline.error" />
              </Text>
              <Text color="grey400" size="sm" as="span">
                {failureMessage}
              </Text>
            </FlexContainer>
          )}
          {jobStatus === "incomplete" && (
            <FlexContainer gap="xs">
              <Text color="yellow600" size="sm" as="span" bold>
                <FormattedMessage id="connection.timeline.warning" />
              </Text>
              <Text color="grey400" size="sm" as="span">
                {failureMessage}
              </Text>
            </FlexContainer>
          )}
        </Box>
      </FlexItem>
      <JobEventMenu eventId={eventId} jobId={jobId} />
    </ConnectionTimelineEventItem>
  );
};
