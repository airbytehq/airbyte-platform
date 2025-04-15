import classnames from "classnames";
import { useMemo } from "react";

import { FlexContainer } from "components/ui/Flex";
import { Link } from "components/ui/Link";
import { Tooltip } from "components/ui/Tooltip";

import { useCurrentWorkspace } from "core/api";
import { DestinationDefinitionRead, SourceDefinitionRead } from "core/api/types/AirbyteClient";
import { isSourceDefinition } from "core/domain/connector/source";
import { useIntent } from "core/utils/rbac";

import styles from "./ConnectionOnboardingConnectorLink.module.scss";

interface ConnectionOnboardingConnectorLinkProps {
  connector?: SourceDefinitionRead | DestinationDefinitionRead;
  testId: string;
  tooltipText: string;
  to: string;
  onMouseEnter: () => void;
  children: React.ReactNode;
}

export const ConnectionOnboardingConnectorLink: React.FC<ConnectionOnboardingConnectorLinkProps> = ({
  connector,
  testId,
  tooltipText,
  to,
  onMouseEnter,
  children,
}) => {
  const { workspaceId } = useCurrentWorkspace();
  const canCreateConnection = useIntent("CreateConnection", { workspaceId });

  const dataDefinitionId = useMemo(() => {
    if (connector) {
      if (isSourceDefinition(connector)) {
        return {
          "data-source-definition-id": connector.sourceDefinitionId,
        };
      }
      return {
        "data-destination-definition-id": connector.destinationDefinitionId,
      };
    }
    return {};
  }, [connector]);

  const ui = (
    <FlexContainer
      className={classnames(styles.connectorButton, canCreateConnection && styles.isLink)}
      onMouseEnter={onMouseEnter}
      alignItems="center"
      justifyContent="center"
    >
      {children}
    </FlexContainer>
  );

  const control = canCreateConnection ? (
    <Link data-testid={testId} aria-label={tooltipText} to={to} {...dataDefinitionId}>
      {ui}
    </Link>
  ) : (
    ui
  );

  return (
    <Tooltip placement="right" control={control}>
      {tooltipText}
    </Tooltip>
  );
};
