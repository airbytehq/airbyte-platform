import React from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { ProgressBar } from "components/ui/ProgressBar";
import { Text } from "components/ui/Text";

import { JobFailure } from "area/connection/components/JobFailure";
import { FailureReason, JobConfigType, LogEvents, LogRead } from "core/api/types/AirbyteClient";

import TestingConnectionSuccess from "./TestingConnectionSuccess";

interface IProps {
  formType: "source" | "destination";
  isValid: boolean;
  onRetestClick: () => void;
  onCancelTesting: () => void;
  isTestConnectionInProgress?: boolean;
  successMessage?: React.ReactNode;
  errorMessage?: React.ReactNode;
  jobId?: string;
  jobConfigType?: JobConfigType;
  jobLogs?: LogEvents | LogRead;
  jobFailureReason?: FailureReason;
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
  jobId,
  jobConfigType,
  jobLogs,
  jobFailureReason,
  isEditMode,
  dirty,
}) => {
  const renderStatusMessage = () => {
    if (errorMessage) {
      return (
        <JobFailure
          fallbackMessage={errorMessage}
          id={jobId}
          configType={jobConfigType}
          logs={jobLogs}
          failureReason={jobFailureReason}
        />
      );
    }
    if (connectionTestSuccess) {
      return <TestingConnectionSuccess />;
    }
    return null;
  };

  return (
    <Card>
      <FlexContainer direction="column">
        <FlexContainer alignItems="center">
          <FlexItem grow>
            <Text size="lg">
              <FormattedMessage id={`form.${formType}RetestTitle`} />
            </Text>
          </FlexItem>
          {isTestConnectionInProgress || !isEditMode ? (
            <Button
              icon="cross"
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
              icon="reset"
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
