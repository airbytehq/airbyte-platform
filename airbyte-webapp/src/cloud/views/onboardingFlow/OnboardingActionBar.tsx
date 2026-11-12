import React from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";

interface OnboardingActionBarProps {
  onSkip?: () => void;
  onBack?: () => void;
  onContinue?: () => void;
  /** Continue is saving the screen, so Continue shows a spinner, and Back and Skip wait. */
  isSaving?: boolean;
}

export const OnboardingActionBar: React.FC<OnboardingActionBarProps> = ({ onSkip, onBack, onContinue, isSaving }) => (
  <FlexContainer alignItems="center" justifyContent="space-between">
    <Button variant="clear" onClick={onSkip} disabled={!onSkip || isSaving}>
      <FormattedMessage id="onboardingFlow.actions.skip" />
    </Button>
    <FlexContainer alignItems="center" gap="md">
      {onBack && (
        <Button variant="secondary" onClick={onBack} disabled={isSaving}>
          <FormattedMessage id="onboardingFlow.actions.back" />
        </Button>
      )}
      <Button onClick={onContinue} disabled={!onContinue} isLoading={isSaving}>
        <FormattedMessage id="onboardingFlow.actions.continue" />
      </Button>
    </FlexContainer>
  </FlexContainer>
);
