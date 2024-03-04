import classnames from "classnames";

import { FlexContainer } from "components/ui/Flex";
import { Link } from "components/ui/Link";
import { Tooltip } from "components/ui/Tooltip";

import { useCurrentWorkspace } from "core/api";
import { DestinationDefinitionSpecification, SourceDefinitionSpecification } from "core/api/types/AirbyteClient";
import { useIntent } from "core/utils/rbac";

import styles from "./ConnectionOnboardingConnectorLink.module.scss";

interface ConnectionOnboardingConnectorLinkProps {
  connector?: SourceDefinitionSpecification | DestinationDefinitionSpecification;
  connectorType?: "source" | "destination";
  testId: string;
  tooltipText: string;
  to: string;
  onMouseEnter: () => void;
  children: React.ReactNode;
}

export const ConnectionOnboardingConnectorLink: React.FC<ConnectionOnboardingConnectorLinkProps> = ({
  connector,
  connectorType,
  testId,
  tooltipText,
  to,
  onMouseEnter,
  children,
}) => {
  const { workspaceId } = useCurrentWorkspace();
  const canCreateConnection = useIntent("CreateConnection", { workspaceId });

  const dataDefinitionId = {
    [`data-${connectorType}-definition-id`]: connector?.[`${connectorType}DefinitionId`],
  };

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
