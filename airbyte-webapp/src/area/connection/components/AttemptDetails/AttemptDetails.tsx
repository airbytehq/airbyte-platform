import React from "react";
import { FormattedDate, FormattedMessage, useIntl } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { AttemptRead, AttemptStats, AttemptStatus, FailureReason, FailureType } from "core/api/types/AirbyteClient";
import { formatBytes } from "core/utils/numberHelper";
import { useFormatLengthOfTime } from "core/utils/time";

import styles from "./AttemptDetails.module.scss";

const getFailureFromAttempt = (attempt: AttemptRead): FailureReason | undefined => attempt.failureSummary?.failures[0];

const isCancelledAttempt = (attempt: AttemptRead): boolean =>
  attempt.failureSummary?.failures.some(({ failureType }) => failureType === FailureType.manual_cancellation) ?? false;

interface AttemptDetailsProps {
  className?: string;
  attempt: AttemptRead;
  hasMultipleAttempts?: boolean;
  jobId: string;
  isPartialSuccess?: boolean;
  showEndedAt?: boolean;
  showFailureMessage?: boolean;
  aggregatedAttemptStats?: AttemptStats;
}

export const AttemptDetails: React.FC<AttemptDetailsProps> = ({
  attempt,
  hasMultipleAttempts,
  jobId,
  isPartialSuccess,
  showEndedAt = false,
  showFailureMessage = true,
  aggregatedAttemptStats,
}) => {
  const { formatMessage } = useIntl();
  const attemptRunTime = useFormatLengthOfTime((attempt.updatedAt - attempt.createdAt) * 1000);

  if (attempt.status !== AttemptStatus.succeeded && attempt.status !== AttemptStatus.failed) {
    return null;
  }

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

  const isCancelled = isCancelledAttempt(attempt);
  const isFailed = attempt.status === AttemptStatus.failed && !isCancelled;

  return (
    <>
      <FlexContainer gap="sm">
        {hasMultipleAttempts && (
          <Text color={isFailed && !isPartialSuccess ? "red" : "darkBlue"} bold as="span" size="sm">
            <FormattedMessage id="sources.lastAttempt" />
          </Text>
        )}
        {showEndedAt && attempt.endedAt && (
          <>
            <Text as="span" color="grey" size="sm">
              <FormattedDate value={attempt.endedAt * 1000} dateStyle="medium" timeStyle="short" />
            </Text>
            <Text as="span" color="grey" size="sm">
              |
            </Text>
          </>
        )}
        <Text as="span" color="grey" size="sm">
          {formatBytes(aggregatedAttemptStats?.bytesEmitted || attempt?.totalStats?.bytesEmitted)}
        </Text>
        <Text as="span" color="grey" size="sm">
          |
        </Text>
        <Text as="span" color="grey" size="sm">
          <FormattedMessage
            id="sources.countRecordsExtracted"
            values={{ count: aggregatedAttemptStats?.recordsEmitted || attempt.totalStats?.recordsEmitted || 0 }}
          />
        </Text>
        <Text as="span" color="grey" size="sm">
          |
        </Text>
        <Text as="span" color="grey" size="sm">
          <FormattedMessage
            id="sources.countRecordsLoaded"
            values={{ count: aggregatedAttemptStats?.recordsCommitted || attempt.totalStats?.recordsCommitted || 0 }}
          />
        </Text>
        <Text as="span" color="grey" size="sm">
          |
        </Text>
        <Text as="span" color="grey" size="sm">
          <FormattedMessage id="jobs.jobId" values={{ id: jobId }} />
        </Text>
        <Text as="span" color="grey" size="sm">
          |
        </Text>
        <Text as="span" color="grey" size="sm">
          {attemptRunTime}
        </Text>
      </FlexContainer>
      {showFailureMessage && isFailed && (
        <Text color={isPartialSuccess ? "grey" : "red"} size="sm" className={styles.failedMessage}>
          {formatMessage(
            {
              id: "ui.keyValuePairV3",
            },
            {
              key: getFailureOrigin(attempt),
              value: getExternalFailureMessage(attempt),
            }
          )}
        </Text>
      )}
    </>
  );
};
