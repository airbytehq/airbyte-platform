import React, { useEffect, useRef } from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { ProgressBar } from "components/ui/ProgressBar";
import { Text } from "components/ui/Text";

import TestingConnectionSuccess from "./TestingConnectionSuccess";

interface IProps {
  formType: "source" | "destination";
  isValid: boolean;
  onRetestClick?: () => void;
  onCancelTesting: () => void;
  isTestConnectionInProgress?: boolean;
  successMessage?: React.ReactNode;
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
  onCancelTesting,
  isEditMode,
  dirty,
}) => {
  const cardRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (isTestConnectionInProgress) {
      cardRef.current?.scrollIntoView({ behavior: "smooth", block: "nearest" });
    }
  }, [isTestConnectionInProgress]);

  return (
    <Card ref={cardRef}>
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
          ) : onRetestClick ? (
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
          ) : null}
        </FlexContainer>
        {isTestConnectionInProgress ? (
          <FlexContainer justifyContent="center">
            <ProgressBar runTime={PROGRESS_BAR_TIME} />
          </FlexContainer>
        ) : (
          connectionTestSuccess && <TestingConnectionSuccess />
        )}
      </FlexContainer>
    </Card>
  );
};
