import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { DropdownMenu, DropdownMenuOptionType } from "components/ui/DropdownMenu";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { ActiveConnectionLimitReachedModal } from "area/workspace/components/ActiveConnectionLimitReachedModal";
import { useCurrentWorkspaceLimits } from "area/workspace/utils/useCurrentWorkspaceLimits";
import { useCurrentWorkspace } from "core/api";
import { useIntent } from "core/utils/rbac";
import { useModalService } from "hooks/services/Modal";

import styles from "./TableItemTitle.module.scss";
import { Button } from "../ui/Button";

interface TableItemTitleProps {
  type: "source" | "destination";
  dropdownOptions: DropdownMenuOptionType[];
  onSelect: (data: DropdownMenuOptionType) => void;
  connectionsCount: number;
}

const TableItemTitle: React.FC<TableItemTitleProps> = ({ type, dropdownOptions, onSelect, connectionsCount }) => {
  const { activeConnectionLimitReached, limits } = useCurrentWorkspaceLimits();
  const { openModal } = useModalService();
  const { formatMessage } = useIntl();
  const { workspaceId } = useCurrentWorkspace();
  const canCreateConnection = useIntent("CreateConnection", { workspaceId });

  const handleButtonClickLimitsReached = () => {
    if (!limits) {
      return;
    }
    openModal({
      title: formatMessage({ id: "workspaces.activeConnectionLimitReached.title" }),
      content: () => <ActiveConnectionLimitReachedModal connectionCount={limits.activeConnections.current} />,
    });
  };

  return (
    <Box px="xl" pt="lg">
      <FlexContainer alignItems="center" justifyContent="space-between">
        <Heading as="h3" size="sm">
          <FormattedMessage id="tables.connections.pluralized" values={{ value: connectionsCount }} />
        </Heading>
        {activeConnectionLimitReached ? (
          <Button onClick={handleButtonClickLimitsReached} disabled={!canCreateConnection}>
            <FormattedMessage id={`tables.${type}Add`} />
          </Button>
        ) : (
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
              <Button disabled={!canCreateConnection} data-testid={`select-${type}`}>
                <FormattedMessage id={`tables.${type}Add`} />
              </Button>
            )}
          </DropdownMenu>
        )}
      </FlexContainer>
    </Box>
  );
};

export default TableItemTitle;
