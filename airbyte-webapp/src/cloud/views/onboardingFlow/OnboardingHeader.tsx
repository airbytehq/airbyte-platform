import React from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { StepsIndicators } from "components/ui/StepsIndicators/StepsIndicators";
import { ThemeToggle } from "components/ui/ThemeToggle";

import { OrganizationPicker } from "area/organization/OrganizationPicker/OrganizationPicker";
import { useAuthService } from "core/services/auth";

import styles from "./OnboardingHeader.module.scss";
import { getOnboardingProgress, OnboardingStepId } from "./onboardingSteps";

interface OnboardingHeaderProps {
  currentStep: OnboardingStepId;
}

export const OnboardingHeader: React.FC<OnboardingHeaderProps> = ({ currentStep }) => {
  const { logout } = useAuthService();
  const steps = getOnboardingProgress(currentStep).map(({ labelId, state }) => ({
    state,
    label: <FormattedMessage id={labelId} />,
  }));

  return (
    <header className={styles.header}>
      <div className={styles.header__organization}>
        <OrganizationPicker />
      </div>
      <div className={styles.header__progress}>
        <StepsIndicators steps={steps} />
      </div>
      <FlexContainer alignItems="center" gap="sm" className={styles.header__userActions}>
        <ThemeToggle />
        {logout && (
          <Button variant="clear" icon="signout" onClick={() => logout()}>
            <FormattedMessage id="sidebar.logout" />
          </Button>
        )}
      </FlexContainer>
    </header>
  );
};
