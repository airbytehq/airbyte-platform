import dayjs from "dayjs";
import React from "react";
import { FormattedDate, FormattedMessage, FormattedTimeParts, useIntl } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { AttemptRead, AttemptStatus, FailureReason, FailureType } from "core/api/types/AirbyteClient";
import { formatBytes } from "core/utils/numberHelper";

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
}

export const AttemptDetails: React.FC<AttemptDetailsProps> = ({
  attempt,
  hasMultipleAttempts,
  jobId,
  isPartialSuccess,
  showEndedAt = false,
  showFailureMessage = true,
}) => {
  const { formatMessage } = useIntl();

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

  const date1 = dayjs(attempt.createdAt * 1000);
  const date2 = dayjs(attempt.updatedAt * 1000);
  const hours = Math.abs(date2.diff(date1, "hour"));
  const minutes = Math.abs(date2.diff(date1, "minute")) - hours * 60;
  const seconds = Math.abs(date2.diff(date1, "second")) - minutes * 60 - hours * 3600;
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
              <FormattedTimeParts value={attempt.createdAt * 1000} hour="numeric" minute="2-digit">
                {(parts) => <span>{`${parts[0].value}:${parts[2].value}${parts[4].value} `}</span>}
              </FormattedTimeParts>
              <FormattedDate value={attempt.createdAt * 1000} month="2-digit" day="2-digit" year="numeric" />
            </Text>
            <Text as="span" color="grey" size="sm">
              |
            </Text>
          </>
        )}
        <Text as="span" color="grey" size="sm">
          {formatBytes(attempt?.totalStats?.bytesEmitted)}
        </Text>
        <Text as="span" color="grey" size="sm">
          |
        </Text>
        <Text as="span" color="grey" size="sm">
          <FormattedMessage
            id="sources.countRecordsExtracted"
            values={{ count: attempt.totalStats?.recordsEmitted || 0 }}
          />
        </Text>
        <Text as="span" color="grey" size="sm">
          |
        </Text>
        <Text as="span" color="grey" size="sm">
          <FormattedMessage
            id="sources.countRecordsLoaded"
            values={{ count: attempt.totalStats?.recordsCommitted || 0 }}
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
          {hours ? <FormattedMessage id="sources.hour" values={{ hour: hours }} /> : null}
          {hours || minutes ? <FormattedMessage id="sources.minute" values={{ minute: minutes }} /> : null}
          <FormattedMessage id="sources.second" values={{ second: seconds }} />
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
