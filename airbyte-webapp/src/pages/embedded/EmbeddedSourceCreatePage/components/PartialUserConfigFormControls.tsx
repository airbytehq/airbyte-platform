import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";

interface PartialUserConfigFormControlsProps {
  isEditMode: boolean;
  isSubmitting: boolean;
  dirty: boolean;
}

export const PartialUserConfigFormControls: React.FC<PartialUserConfigFormControlsProps> = ({
  isEditMode,
  isSubmitting,
  dirty,
}) => {
  console.log("isEditMode", isEditMode);
  console.log("dirty", dirty);
  console.log("isSubmitting", isSubmitting);

  return (
    <FlexContainer justifyContent="flex-end">
      <Button full type="submit" disabled={isSubmitting || !dirty} isLoading={isSubmitting}>
        {isEditMode ? (
          <FormattedMessage id="form.saveChanges" />
        ) : (
          <FormattedMessage id="onboarding.sourceSetUp.buttonText" />
        )}
      </Button>
    </FlexContainer>
  );
};
