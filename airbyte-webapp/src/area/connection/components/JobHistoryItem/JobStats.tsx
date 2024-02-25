import dayjs from "dayjs";
import { FormattedMessage, useIntl } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { JobWithAttempts } from "area/connection/types/jobs";
import { isJobPartialSuccess } from "area/connection/utils/jobs";
import { AttemptRead, FailureReason } from "core/api/types/AirbyteClient";
import { formatBytes } from "core/utils/numberHelper";

import styles from "./JobStats.module.scss";

interface JobStatsProps {
  jobWithAttempts: JobWithAttempts;
}

export const JobStats: React.FC<JobStatsProps> = ({ jobWithAttempts }) => {
  const { formatMessage } = useIntl();

  const { job, attempts } = jobWithAttempts;
  const isPartialSuccess = isJobPartialSuccess(jobWithAttempts.attempts);
  const lastAttempt = attempts && attempts[attempts.length - 1];

  const start = dayjs(job.createdAt * 1000);
  const end = dayjs(job.updatedAt * 1000);
  const hours = Math.abs(end.diff(start, "hour"));
  const minutes = Math.abs(end.diff(start, "minute")) - hours * 60;
  const seconds = Math.abs(end.diff(start, "second")) - minutes * 60 - hours * 3600;

  const getFailureFromAttempt = (attempt: AttemptRead): FailureReason | undefined =>
    attempt.failureSummary?.failures[0];

  const getFailureOrigin = (attempt: AttemptRead) => {
    const failure = getFailureFromAttempt(attempt);
    const failureOrigin = failure?.failureOrigin ?? formatMessage({ id: "errorView.unknown" });

    return `${formatMessage({
      id: "sources.failureOrigin",
    })}: ${failureOrigin}`;
  };

  const getExternalFailureMessage = (attempt: AttemptRead) => {
    const failure = getFailureFromAttempt(attempt);
    const failureMessage = failure?.externalMessage ?? formatMessage({ id: "errorView.unknown" });

    return `${formatMessage({
      id: "sources.message",
    })}: ${failureMessage}`;
  };

  if (job.status === "running") {
    return null;
  }

  return (
    <>
      <FlexContainer gap="sm">
        {!job.aggregatedStats && (
          <Text color="grey" size="sm">
            <FormattedMessage id="jobs.noMetadataAvailable" />
          </Text>
        )}
        {job.aggregatedStats && (
          <>
            <Text as="span" color="grey" size="sm">
              {formatBytes(job.aggregatedStats.bytesEmitted)}
            </Text>
            <Text as="span" color="grey" size="sm">
              |
            </Text>
            <Text as="span" color="grey" size="sm">
              <FormattedMessage
                id="sources.countRecordsExtracted"
                values={{ count: job.aggregatedStats.recordsEmitted || 0 }}
              />
            </Text>
            <Text as="span" color="grey" size="sm">
              |
            </Text>
            <Text as="span" color="grey" size="sm">
              <FormattedMessage
                id="sources.countRecordsLoaded"
                values={{ count: job.aggregatedStats.recordsCommitted || 0 }}
              />
            </Text>
            <Text as="span" color="grey" size="sm">
              |
            </Text>
            <Text as="span" color="grey" size="sm">
              <FormattedMessage id="jobs.jobId" values={{ id: job.id }} />
            </Text>
            <Text as="span" color="grey" size="sm">
              |
            </Text>
            <Text as="span" color="grey" size="sm">
              {hours ? <FormattedMessage id="sources.hour" values={{ hour: hours }} /> : null}
              {hours || minutes ? <FormattedMessage id="sources.minute" values={{ minute: minutes }} /> : null}
              <FormattedMessage id="sources.second" values={{ second: seconds }} />
            </Text>
          </>
        )}
      </FlexContainer>
      {job.status === "failed" && lastAttempt && (
        <Text color={isPartialSuccess ? "grey" : "red"} size="sm" className={styles.failedMessage}>
          {formatMessage(
            {
              id: "ui.keyValuePairV3",
            },
            {
              key: getFailureOrigin(lastAttempt),
              value: getExternalFailureMessage(lastAttempt),
            }
          )}
        </Text>
      )}
    </>
  );
};
