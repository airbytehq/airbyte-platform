import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Link } from "components/ui/Link";

import { ActiveConnectionLimitReachedModal } from "area/workspace/components/ActiveConnectionLimitReachedModal";
import { useCurrentWorkspaceLimits } from "area/workspace/utils/useCurrentWorkspaceLimits";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
import { useModalService } from "hooks/services/Modal";

interface TableItemTitleProps {
  createConnectionLink: string;
  connectionsCount: number;
}

const TableItemTitle: React.FC<TableItemTitleProps> = ({ createConnectionLink, connectionsCount }) => {
  const { activeConnectionLimitReached, limits } = useCurrentWorkspaceLimits();
  const { openModal } = useModalService();
  const { formatMessage } = useIntl();
  const canCreateConnection = useGeneratedIntent(Intent.CreateOrEditConnection);

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
        {canCreateConnection && (
          <Link
            to={createConnectionLink}
            variant="buttonPrimary"
            onClick={() => (activeConnectionLimitReached ? handleButtonClickLimitsReached() : undefined)}
          >
            <FormattedMessage id="connector.connections.empty.button" />
          </Link>
        )}
      </FlexContainer>
    </Box>
  );
};

export default TableItemTitle;
