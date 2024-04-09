import { FormattedMessage, useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Message } from "components/ui/Message";

import { AttemptFailureSummary, FailureType } from "core/api/types/AirbyteClient";
import { copyToClipboard } from "core/utils/clipboard";
import { failureUiDetailsFromReason } from "core/utils/errorStatusMessage";
import { useNotificationService } from "hooks/services/Notification";

import styles from "./JobLogsModalFailureMessage.module.scss";

interface JobLogsModalFailureMessageProps {
  failureSummary?: AttemptFailureSummary;
}

export const JobLogsModalFailureMessage: React.FC<JobLogsModalFailureMessageProps> = ({ failureSummary }) => {
  const { registerNotification } = useNotificationService();
  const { formatMessage } = useIntl();
  const failureUiDetails = failureUiDetailsFromReason(failureSummary?.failures[0], formatMessage);

  const isFailureCancellation = failureSummary?.failures.some(
    ({ failureType }) => failureType === FailureType.manual_cancellation
  );
  const showFailureMessage = !isFailureCancellation && failureUiDetails;

  if (!showFailureMessage) {
    return null;
  }

  const onCopyTextBtnClick = async () => {
    if (!failureUiDetails.secondaryMessage) {
      return;
    }
    await copyToClipboard(failureUiDetails.secondaryMessage);

    registerNotification({
      type: "success",
      text: formatMessage({ id: "jobs.failure.copyText.success" }),
      id: "jobs.failure.copyText.success",
    });
  };

  return (
    <Box px="md">
      <div className={styles.internalFailureContainer}>
        <Message
          type={failureUiDetails.type}
          text={
            <FlexContainer justifyContent="space-between" alignItems="center">
              <FormattedMessage
                id="failureMessage.label"
                values={{ type: `${failureUiDetails.typeLabel}:`, message: failureUiDetails.message }}
              />

              {failureUiDetails.secondaryMessage && (
                <Button onClick={onCopyTextBtnClick}>
                  <FormattedMessage id="jobs.failure.copyText" />
                </Button>
              )}
            </FlexContainer>
          }
        >
          {failureUiDetails.secondaryMessage}
        </Message>
      </div>
    </Box>
  );
};
