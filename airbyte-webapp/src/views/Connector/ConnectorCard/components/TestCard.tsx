import React from "react";
import { FormattedMessage } from "react-intl";

import { JobFailure } from "components/JobFailure";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { ProgressBar } from "components/ui/ProgressBar";
import { Text } from "components/ui/Text";

import { SynchronousJobRead } from "core/api/types/AirbyteClient";

import styles from "./TestCard.module.scss";
import TestingConnectionSuccess from "./TestingConnectionSuccess";

interface IProps {
  formType: "source" | "destination";
  isValid: boolean;
  onRetestClick: () => void;
  onCancelTesting: () => void;
  isTestConnectionInProgress?: boolean;
  successMessage?: React.ReactNode;
  errorMessage?: React.ReactNode;
  job?: SynchronousJobRead;
  isEditMode?: boolean;
  dirty: boolean;
  connectionTestSuccess: boolean;
}

const PROGRESS_BAR_TIME = 60 * 2;

export const TestCard: React.FC<IProps> = ({
  isTestConnectionInProgress,
  isValid,
  formType,
  onRetestClick,
  connectionTestSuccess,
  errorMessage,
  onCancelTesting,
  job,
  isEditMode,
  dirty,
}) => {
  const renderStatusMessage = () => {
    if (errorMessage) {
      return <JobFailure job={job} fallbackMessage={errorMessage} />;
    }
    if (connectionTestSuccess) {
      return <TestingConnectionSuccess />;
    }
    return null;
  };

  return (
    <Card className={styles.cardTest}>
      <FlexContainer direction="column">
        <FlexContainer alignItems="center">
          <FlexItem grow>
            <Text size="lg">
              <FormattedMessage id={`form.${formType}RetestTitle`} />
            </Text>
          </FlexItem>
          {isTestConnectionInProgress || !isEditMode ? (
            <Button
              icon={<Icon type="cross" />}
              variant="secondary"
              type="button"
              disabled={!isTestConnectionInProgress}
              onClick={() => onCancelTesting?.()}
            >
              <FormattedMessage id="form.cancel" />
            </Button>
          ) : (
            <Button
              type="button"
              onClick={onRetestClick}
              variant="secondary"
              icon={<Icon type="reset" />}
              // disable if there are changes in edit mode because the retest API can currently only test the saved state
              disabled={!isValid || dirty}
            >
              <FormattedMessage id={`form.${formType}Retest`} />
            </Button>
          )}
        </FlexContainer>
        {isTestConnectionInProgress ? (
          <FlexContainer justifyContent="center">
            <ProgressBar runTime={PROGRESS_BAR_TIME} />
          </FlexContainer>
        ) : (
          renderStatusMessage()
        )}
      </FlexContainer>
    </Card>
  );
};
