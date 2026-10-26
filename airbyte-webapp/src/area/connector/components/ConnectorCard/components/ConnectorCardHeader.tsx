import React from "react";

import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { ConnectorIcon } from "area/connector/components/ConnectorIcon";
import { CloudInviteUsersButton } from "area/organization/components/CloudInviteUsersButton";
import { ConnectorDefinition } from "core/domain/connector";
import { isSourceDefinition } from "core/domain/connector/source";
import { ForkConnectorButton } from "pages/connectorBuilder/components/ForkConnectorButton";

import styles from "./ConnectorCardHeader.module.scss";

interface ConnectorCardHeaderProps {
  connectorDefinition: ConnectorDefinition;
  actionsDisabled?: boolean;
}

export const ConnectorCardHeader: React.FC<ConnectorCardHeaderProps> = ({ connectorDefinition, actionsDisabled }) => (
  <FlexContainer alignItems="center" justifyContent="space-between">
    <FlexContainer alignItems="center" gap="md">
      <ConnectorIcon icon={connectorDefinition.icon} className={styles.icon} />
      <Text as="h2" size="lg" color="grey500">
        {connectorDefinition.name}
      </Text>
    </FlexContainer>
    <FlexContainer alignItems="center" gap="sm">
      {isSourceDefinition(connectorDefinition) && (
        <ForkConnectorButton sourceDefinition={connectorDefinition} iconOnly disabled={actionsDisabled} />
      )}
      <CloudInviteUsersButton disabled={actionsDisabled} />
    </FlexContainer>
  </FlexContainer>
);
