import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { DropdownMenu, DropdownMenuOptionType } from "components/ui/DropdownMenu";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import styles from "./TableItemTitle.module.scss";
import { Button } from "../ui/Button";

interface TableItemTitleProps {
  type: "source" | "destination";
  dropdownOptions: DropdownMenuOptionType[];
  onSelect: (data: DropdownMenuOptionType) => void;
  connectionsCount: number;
}

const TableItemTitle: React.FC<TableItemTitleProps> = ({ type, dropdownOptions, onSelect, connectionsCount }) => {
  const { formatMessage } = useIntl();
  return (
    <Box px="xl" pt="lg">
      <FlexContainer alignItems="center" justifyContent="space-between">
        <Heading as="h3" size="sm">
          <FormattedMessage id="tables.connections.pluralized" values={{ value: connectionsCount }} />
        </Heading>
        <DropdownMenu
          placement="bottom-end"
          options={[
            {
              as: "button",
              className: styles.primary,
              value: "create-new-item",
              displayName: formatMessage({
                id: `tables.${type}AddNew`,
              }),
            },
            ...dropdownOptions,
          ]}
          onChange={onSelect}
        >
          {() => (
            <Button data-testid={`select-${type}`}>
              <FormattedMessage id={`tables.${type}Add`} />
            </Button>
          )}
        </DropdownMenu>
      </FlexContainer>
    </Box>
  );
};

export default TableItemTitle;
