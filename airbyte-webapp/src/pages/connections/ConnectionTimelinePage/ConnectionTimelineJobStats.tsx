import classNames from "classnames";
import dayjs from "dayjs";
import { useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { failureUiDetailsFromReason } from "core/utils/errorStatusMessage";
import { formatBytes } from "core/utils/numberHelper";
import { useFormatLengthOfTime } from "core/utils/time";
import { useLocalStorage } from "core/utils/useLocalStorage";

import styles from "./ConnectionTimelineJobStats.module.scss";
import { ConnectionTimelineJobStatsProps } from "./utils";

export const ConnectionTimelineJobStats: React.FC<ConnectionTimelineJobStatsProps> = ({
  bytesCommitted,
  recordsCommitted,
  jobId,
  failureSummary,
  jobStartedAt,
  jobEndedAt,
  attemptsCount,
  jobStatus,
}) => {
  const { formatMessage } = useIntl();

  const [showExtendedStats] = useLocalStorage("airbyte_extended-attempts-stats", false);

  const start = dayjs(jobStartedAt * 1000);
  const end = dayjs(jobEndedAt * 1000);
  const duration = end.diff(start, "milliseconds");
  const timeToShow = useFormatLengthOfTime(duration);

  const [isSecondaryMessageExpanded, setIsSecondaryMessageExpanded] = useState(false);

  const failureUiDetails = failureUiDetailsFromReason(failureSummary, formatMessage);

  return (
    <>
      <FlexContainer gap="sm">
        {bytesCommitted && recordsCommitted && (
          <>
            <Text as="span" color="grey500" size="sm">
              {formatBytes(bytesCommitted)}
            </Text>
            <Text as="span" color="grey500" size="sm">
              |
            </Text>
            <Text as="span" color="grey500" size="sm">
              <FormattedMessage id="sources.countRecordsLoaded" values={{ count: recordsCommitted }} />
            </Text>
            <Text as="span" color="grey500" size="sm">
              |
            </Text>
          </>
        )}
        <Text as="span" color="grey500" size="sm">
          {timeToShow}
        </Text>
        {showExtendedStats && (
          <>
            <Text as="span" color="grey500" size="sm">
              |
            </Text>
            <Text as="span" color="grey500" size="sm">
              <FormattedMessage id="jobs.jobId" values={{ id: jobId }} />
            </Text>
            <Text as="span" color="grey500" size="sm">
              |
            </Text>
            <Text as="span" color="grey500" size="sm">
              <FormattedMessage id="jobs.attemptCount" values={{ count: attemptsCount }} />
            </Text>
          </>
        )}
      </FlexContainer>
      {jobStatus === "failed" && failureUiDetails && (
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
          className={classNames({
            [styles["secondaryMessage--error"]]: failureUiDetails.type === "error",
            [styles["secondaryMessage--warning"]]: failureUiDetails.type === "warning",
          })}
        >
          {failureUiDetails.secondaryMessage}
        </Text>
      )}
    </>
  );
};
