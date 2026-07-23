import React from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

interface EditorHeaderProps {
  mainTitle?: React.ReactNode;
  addButtonText?: React.ReactNode;
  itemsCount: number;
  onAddItem: () => void;
}

export const EditorHeader: React.FC<EditorHeaderProps> = ({ itemsCount, onAddItem, mainTitle, addButtonText }) => {
  return (
    <Box mt="sm" mb="md">
      <FlexContainer justifyContent="space-between" alignItems="center">
        <Text color="darkBlue" size="lg" bold>
          {mainTitle || <FormattedMessage id="form.items" values={{ count: itemsCount }} />}
        </Text>
        <Button variant="secondary" type="button" onClick={onAddItem} data-testid="addItemButton">
          {addButtonText || <FormattedMessage id="form.addItems" />}
        </Button>
      </FlexContainer>
    </Box>
  );
};
