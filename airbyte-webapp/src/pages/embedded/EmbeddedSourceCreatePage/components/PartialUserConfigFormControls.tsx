import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";

interface PartialUserConfigFormControlsProps {
  isEditMode: boolean;
  isSubmitting: boolean;
  isValid: boolean;
  dirty: boolean;

  onCancel: () => void;
}

export const PartialUserConfigFormControls: React.FC<PartialUserConfigFormControlsProps> = ({
  isEditMode,
  isSubmitting,
  isValid,
  dirty,
}) => {
  return (
    <FlexContainer justifyContent="flex-end">
      <Button full type="submit" disabled={!isValid || !dirty || isSubmitting} isLoading={isSubmitting}>
        {isEditMode ? (
          <FormattedMessage id="form.saveChanges" />
        ) : (
          <FormattedMessage id="onboarding.sourceSetUp.buttonText" />
        )}
      </Button>
    </FlexContainer>
  );
};
