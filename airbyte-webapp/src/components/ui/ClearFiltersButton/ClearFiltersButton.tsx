import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";

interface ClearFiltersButtonProps {
  onClick: () => void;
}

export const ClearFiltersButton: React.FC<ClearFiltersButtonProps> = ({ onClick }) => {
  return (
    <Button variant="clear" onClick={onClick}>
      <FlexContainer alignItems="center" gap="sm">
        <Icon type="cross" size="md" />
        <FormattedMessage id="tables.connections.filters.clear" />
      </FlexContainer>
    </Button>
  );
};
