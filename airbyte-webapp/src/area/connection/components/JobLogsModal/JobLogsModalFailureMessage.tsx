import { useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Message } from "components/ui/Message";

import { AttemptFailureSummary, FailureType } from "core/api/types/AirbyteClient";
import { copyToClipboard } from "core/utils/clipboard";
import { useNotificationService } from "hooks/services/Notification";

import styles from "./JobLogsModalFailureMessage.module.scss";

interface JobLogsModalFailureMessageProps {
  failureSummary?: AttemptFailureSummary;
}

export const JobLogsModalFailureMessage: React.FC<JobLogsModalFailureMessageProps> = ({ failureSummary }) => {
  const { registerNotification } = useNotificationService();
  const { formatMessage } = useIntl();

  const internalFailureReason = useMemo(() => failureSummary?.failures[0]?.internalMessage, [failureSummary]);

  const externalFailureReason = useMemo(() => failureSummary?.failures[0]?.externalMessage, [failureSummary]);

  const failureToShow = useMemo(
    () =>
      !failureSummary ||
      failureSummary?.failures.some(({ failureType }) => failureType === FailureType.manual_cancellation)
        ? "none"
        : failureSummary?.failures[0]?.internalMessage
        ? "internal"
        : failureSummary?.failures[0]?.externalMessage
        ? "external"
        : "unknown",
    [failureSummary]
  );

  if (failureToShow === "none") {
    return null;
  }

  const onCopyTextBtnClick = async () => {
    if (!internalFailureReason) {
      return;
    }
    await copyToClipboard(internalFailureReason);

    registerNotification({
      type: "success",
      text: formatMessage({ id: "jobs.failure.copyText.success" }),
      id: "jobs.failure.copyText.success",
    });
  };

  return (
    <Box px="md" pb="md">
      {failureToShow === "internal" && (
        <div className={styles.internalFailureContainer}>
          <Message
            type="error"
            text={
              <FlexContainer justifyContent="space-between" alignItems="center">
                <FormattedMessage id="jobHistory.logs.failureReason" values={{ reason: externalFailureReason }} />

                <Button onClick={onCopyTextBtnClick}>
                  <FormattedMessage id="jobs.failure.copyText" />
                </Button>
              </FlexContainer>
            }
          />

          <Box px="md">
            <div className={styles.internalFailureReason}>{internalFailureReason}</div>
          </Box>
        </div>
      )}

      {failureToShow === "external" && (
        <Message
          type="error"
          text={<FormattedMessage id="jobHistory.logs.failureReason" values={{ reason: externalFailureReason }} />}
        />
      )}

      {failureToShow === "unknown" && <Message type="error" text={<FormattedMessage id="errorView.unknown" />} />}
    </Box>
  );
};
