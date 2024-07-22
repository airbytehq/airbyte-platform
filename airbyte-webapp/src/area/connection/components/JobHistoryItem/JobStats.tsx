import classNames from "classnames";
import { useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { JobWithAttempts } from "area/connection/types/jobs";
import { failureUiDetailsFromReason } from "core/utils/errorStatusMessage";
import { formatBytes } from "core/utils/numberHelper";
import { useFormatLengthOfTime } from "core/utils/time";
import { useLocalStorage } from "core/utils/useLocalStorage";

import styles from "./JobStats.module.scss";

interface JobStatsProps {
  jobWithAttempts: JobWithAttempts;
}

export const JobStats: React.FC<JobStatsProps> = ({ jobWithAttempts }) => {
  const { formatMessage } = useIntl();

  const [showExtendedStats] = useLocalStorage("airbyte_extended-attempts-stats", false);

  const { job, attempts } = jobWithAttempts;
  const lastAttempt = attempts?.at(-1); // even if attempts is present it might be empty, which `.at` propagates to `lastAttempt`

  const jobRunTime = useFormatLengthOfTime((job.updatedAt - job.createdAt) * 1000);

  const [isSecondaryMessageExpanded, setIsSecondaryMessageExpanded] = useState(false);

  if (job.status === "running") {
    return null;
  }

  const failureUiDetails = failureUiDetailsFromReason(lastAttempt?.failureSummary?.failures[0], formatMessage);

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
            <Text as="span" color="grey500" size="sm">
              {formatBytes(job.aggregatedStats.bytesEmitted)}
            </Text>
            <Text as="span" color="grey500" size="sm">
              |
            </Text>
            <Text as="span" color="grey500" size="sm">
              <FormattedMessage
                id="sources.countRecordsLoaded"
                values={{ count: job.aggregatedStats.recordsCommitted || 0 }}
              />
            </Text>
            <Text as="span" color="grey500" size="sm">
              |
            </Text>
            <Text as="span" color="grey500" size="sm">
              {jobRunTime}
            </Text>
            {showExtendedStats && (
              <>
                <Text as="span" color="grey500" size="sm">
                  |
                </Text>
                <Text as="span" color="grey500" size="sm">
                  <FormattedMessage id="jobs.jobId" values={{ id: job.id }} />
                </Text>
                <Text as="span" color="grey500" size="sm">
                  |
                </Text>
                <Text as="span" color="grey500" size="sm">
                  <FormattedMessage id="jobs.attemptCount" values={{ count: attempts.length }} />
                </Text>
              </>
            )}
          </>
        )}
      </FlexContainer>
      {job.status === "failed" && failureUiDetails && (
        <Text color="grey500" size="sm" className={styles.failedMessage}>
          {formatMessage(
            { id: "failureMessage.label" },
            {
              type: (
                <Text size="sm" color={failureUiDetails.type === "error" ? "red400" : "yellow600"} as="span">
                  {failureUiDetails.typeLabel}:
                </Text>
              ),
              message: failureUiDetails.message,
            }
          )}
          {failureUiDetails?.secondaryMessage && (
            <>
              &nbsp;
              <Button
                variant="link"
                className={styles.seeMore}
                iconPosition="right"
                onClick={() => setIsSecondaryMessageExpanded((expanded) => !expanded)}
              >
                <FormattedMessage id={isSecondaryMessageExpanded ? "jobs.failure.seeLess" : "jobs.failure.seeMore"} />
                <Icon
                  type={isSecondaryMessageExpanded ? "chevronDown" : "chevronRight"}
                  size="sm"
                  className={styles.seeMoreIcon}
                />
              </Button>
            </>
          )}
        </Text>
      )}
      {failureUiDetails && isSecondaryMessageExpanded && (
        <Text
          size="sm"
          className={classNames(styles.secondaryMessage, {
            [styles.errorMessage]: failureUiDetails.type === "error",
            [styles.warningMessage]: failureUiDetails.type === "warning",
          })}
        >
          {failureUiDetails.secondaryMessage}
        </Text>
      )}
    </>
  );
};
