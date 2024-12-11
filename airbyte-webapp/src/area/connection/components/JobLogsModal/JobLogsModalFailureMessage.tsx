import { FormattedMessage, useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { Collapsible } from "components/ui/Collapsible";
import { FlexContainer } from "components/ui/Flex";
import { Message } from "components/ui/Message";

import { AttemptFailureSummary, FailureType } from "core/api/types/AirbyteClient";
import { failureUiDetailsFromReason } from "core/utils/errorStatusMessage";

import styles from "./JobLogsModalFailureMessage.module.scss";

interface JobLogsModalFailureMessageProps {
  failureSummary?: AttemptFailureSummary;
}

export const JobLogsModalFailureMessage: React.FC<JobLogsModalFailureMessageProps> = ({ failureSummary }) => {
  const { formatMessage } = useIntl();
  const failureUiDetails = failureUiDetailsFromReason(failureSummary?.failures[0], formatMessage);

  const isFailureCancellation = failureSummary?.failures.some(
    ({ failureType }) => failureType === FailureType.manual_cancellation
  );
  const showFailureMessage = !isFailureCancellation && failureUiDetails;

  if (!showFailureMessage) {
    return null;
  }

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
            </FlexContainer>
          }
        >
          {failureUiDetails.secondaryMessage && (
            <Collapsible label={formatMessage({ id: "jobHistory.logs.moreDetails" })} initiallyOpen={false}>
              {failureUiDetails.secondaryMessage}
            </Collapsible>
          )}
        </Message>
      </div>
    </Box>
  );
};
