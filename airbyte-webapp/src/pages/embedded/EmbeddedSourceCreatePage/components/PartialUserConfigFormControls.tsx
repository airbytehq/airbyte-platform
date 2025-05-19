import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";

interface PartialUserConfigFormControlsProps {
  isSubmitting: boolean;
}

export const PartialUserConfigFormControls: React.FC<PartialUserConfigFormControlsProps> = ({ isSubmitting }) => {
  return (
    <FlexContainer justifyContent="flex-end">
      <Button full type="submit" disabled={isSubmitting} isLoading={isSubmitting}>
        <FormattedMessage id="partialUserConfig.buttonText" />
      </Button>
    </FlexContainer>
  );
};
