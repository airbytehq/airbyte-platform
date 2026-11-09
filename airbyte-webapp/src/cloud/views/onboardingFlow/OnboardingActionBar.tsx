import React from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";

interface OnboardingActionBarProps {
  onSkip?: () => void;
  onBack?: () => void;
  onContinue?: () => void;
}

export const OnboardingActionBar: React.FC<OnboardingActionBarProps> = ({ onSkip, onBack, onContinue }) => (
  <FlexContainer alignItems="center" justifyContent="space-between">
    <Button variant="clear" onClick={onSkip} disabled={!onSkip}>
      <FormattedMessage id="onboardingFlow.actions.skip" />
    </Button>
    <FlexContainer alignItems="center" gap="md">
      {onBack && (
        <Button variant="secondary" onClick={onBack}>
          <FormattedMessage id="onboardingFlow.actions.back" />
        </Button>
      )}
      <Button onClick={onContinue} disabled={!onContinue}>
        <FormattedMessage id="onboardingFlow.actions.continue" />
      </Button>
    </FlexContainer>
  </FlexContainer>
);
