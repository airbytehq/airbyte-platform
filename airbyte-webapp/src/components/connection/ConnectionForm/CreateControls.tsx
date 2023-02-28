import React from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";

import styles from "./CreateControls.module.scss";

interface CreateControlsProps {
  isSubmitting: boolean;
  isValid: boolean;
  errorMessage?: React.ReactNode;
}

export const CreateControls: React.FC<CreateControlsProps> = ({ isSubmitting, errorMessage, isValid }) => {
  return (
    <FlexContainer justifyContent="space-between" gap="xl" className={styles.container}>
      <div className={styles.errorText}>{errorMessage}</div>
      <div>
        <Button type="submit" isLoading={isSubmitting} disabled={isSubmitting || !isValid}>
          <FormattedMessage id="onboarding.setUpConnection" />
        </Button>
      </div>
    </FlexContainer>
  );
};
