import classNames from "classnames";
import { useState, useRef, useEffect } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { FailureUiDetails } from "core/utils/errorStatusMessage";

import styles from "./JobFailureDetails.module.scss";

interface JobFailureDetailsProps {
  failureUiDetails: FailureUiDetails;
}

export const JobFailureDetails: React.FC<JobFailureDetailsProps> = ({ failureUiDetails }) => {
  const [isSecondaryMessageExpanded, setIsSecondaryMessageExpanded] = useState(false);
  const [isMessageTruncated, setIsMessageTruncated] = useState(false);
  const messageRef = useRef<HTMLDivElement>(null);
  const { formatMessage } = useIntl();

  useEffect(() => {
    if (messageRef.current) {
      setIsMessageTruncated(messageRef.current.scrollWidth > messageRef.current.clientWidth);
    }
  }, [failureUiDetails.message]);

  return (
    <Box pt="xs" className={styles.details}>
      <FlexContainer gap="xs" alignItems="flex-end">
        <Text color="grey500" size="sm" className={styles.failedMessage} ref={messageRef}>
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
        </Text>

        {(failureUiDetails?.secondaryMessage || isMessageTruncated) && (
          <Text color="grey500" size="sm" as="span">
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
          </Text>
        )}
      </FlexContainer>
      {isSecondaryMessageExpanded && (
        <FlexContainer direction="column" gap="md" className={styles.expandedMessage}>
          {isMessageTruncated && (
            <Text
              size="sm"
              className={classNames({
                [styles.errorMessage]: failureUiDetails.type === "error",
                [styles.warningMessage]: failureUiDetails.type === "warning",
              })}
            >
              {failureUiDetails.message}
            </Text>
          )}

          <Text color="grey500" size="sm" className={styles.secondaryMessage}>
            {failureUiDetails.secondaryMessage}
          </Text>
        </FlexContainer>
      )}
    </Box>
  );
};
