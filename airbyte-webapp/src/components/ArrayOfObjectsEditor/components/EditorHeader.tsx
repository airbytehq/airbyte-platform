import React from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { ConnectionFormMode } from "hooks/services/ConnectionForm/ConnectionFormService";

interface EditorHeaderProps {
  mainTitle?: React.ReactNode;
  addButtonText?: React.ReactNode;
  itemsCount: number;
  onAddItem: () => void;
  /**
   * seems like "mode" and "disabled" props can be removed since we can control fields enable/disable states on higher levels
   * TODO: remove during ArrayOfObjectsEditor refactoring and CreateConnectionForm migration
   */
  mode?: ConnectionFormMode;
  disabled?: boolean;
}

export const EditorHeader: React.FC<EditorHeaderProps> = ({
  itemsCount,
  onAddItem,
  mainTitle,
  addButtonText,
  mode,
  disabled,
}) => {
  return (
    <Box mt="sm" mb="md">
      <FlexContainer justifyContent="space-between" alignItems="center">
        <Text color="darkBlue" size="lg" bold>
          {mainTitle || <FormattedMessage id="form.items" values={{ count: itemsCount }} />}
        </Text>
        {mode !== "readonly" && (
          <Button variant="secondary" type="button" onClick={onAddItem} data-testid="addItemButton" disabled={disabled}>
            {addButtonText || <FormattedMessage id="form.addItems" />}
          </Button>
        )}
      </FlexContainer>
    </Box>
  );
};
