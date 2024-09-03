import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { formatBytes } from "core/utils/numberHelper";
import { useFormatDuration } from "core/utils/time";
import { useLocalStorage } from "core/utils/useLocalStorage";

interface JobStatsProps {
  attemptsCount?: number;
  bytesLoaded?: number;
  endTimeEpochSeconds: number;
  jobId: number;
  recordsLoaded?: number;
  startTimeEpochSeconds: number;
}

export const JobStats: React.FC<JobStatsProps> = ({
  attemptsCount,
  bytesLoaded,
  endTimeEpochSeconds,
  jobId,
  recordsLoaded,
  startTimeEpochSeconds,
}) => {
  const [showExtendedStats] = useLocalStorage("airbyte_extended-attempts-stats", false);
  const duration = useFormatDuration(startTimeEpochSeconds * 1000, endTimeEpochSeconds * 1000);

  return (
    <FlexContainer gap="sm">
      {bytesLoaded !== undefined && (
        <>
          <Text as="span" color="grey400" size="sm">
            {formatBytes(bytesLoaded)}
          </Text>
          <StatSeparator />
        </>
      )}
      {!!recordsLoaded !== undefined && (
        <>
          <Text as="span" color="grey400" size="sm">
            <FormattedMessage id="sources.countRecordsLoaded" values={{ count: recordsLoaded }} />
          </Text>
          <StatSeparator />
        </>
      )}
      <Text as="span" color="grey400" size="sm">
        {duration}
      </Text>
      {showExtendedStats && (
        <>
          <StatSeparator />
          <Text as="span" color="grey400" size="sm">
            <FormattedMessage id="jobs.jobId" values={{ id: jobId }} />
          </Text>
          <StatSeparator />
          <Text as="span" color="grey400" size="sm">
            <FormattedMessage id="jobs.attemptCount" values={{ count: attemptsCount }} />
          </Text>
        </>
      )}
    </FlexContainer>
  );
};

export const StatSeparator = () => (
  <Text as="span" color="grey400" size="sm">
    |
  </Text>
);
