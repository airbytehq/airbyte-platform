import classNames from "classnames";
import { useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { FailureUiDetails } from "core/utils/errorStatusMessage";

import styles from "./JobFailureDetails.module.scss";

export const JobFailureDetails: React.FC<{ failureUiDetails: FailureUiDetails }> = ({ failureUiDetails }) => {
  const [isSecondaryMessageExpanded, setIsSecondaryMessageExpanded] = useState(false);
  const { formatMessage } = useIntl();

  return (
    <>
      <FlexContainer gap="xs" alignItems="flex-end">
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
        </Text>

        {failureUiDetails?.secondaryMessage && (
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
