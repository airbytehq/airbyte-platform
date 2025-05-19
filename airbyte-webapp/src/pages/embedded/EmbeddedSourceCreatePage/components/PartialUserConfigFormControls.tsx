import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";

interface PartialUserConfigFormControlsProps {
  isSubmitting: boolean;
  onDelete?: () => void;
}

export const PartialUserConfigFormControls: React.FC<PartialUserConfigFormControlsProps> = ({
  isSubmitting,
  onDelete,
}) => {
  return (
    <FlexContainer justifyContent="flex-end">
      {onDelete && (
        <Button full type="button" variant="danger" disabled={isSubmitting} onClick={onDelete}>
          <FlexContainer justifyContent="center">
            <FormattedMessage id="partialUserConfig.delete.buttonText" />
          </FlexContainer>
        </Button>
      )}
      <Button full type="submit" disabled={isSubmitting} isLoading={isSubmitting}>
        <FormattedMessage id="partialUserConfig.buttonText" />
      </Button>
    </FlexContainer>
  );
};
