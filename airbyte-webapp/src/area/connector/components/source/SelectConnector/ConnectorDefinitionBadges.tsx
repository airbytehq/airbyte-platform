import { Suspense } from "react";
import { FormattedMessage } from "react-intl";

import { Badge } from "components/ui/Badge";
import { FlexContainer } from "components/ui/Flex";

import { useIsAdpOrganization } from "area/organization/utils";
import { useAgentsSupportedDestinationDefinitionIds, useAgentsSupportedSourceDefinitionIds } from "core/api";
import { ConnectorDefinitionOrEnterpriseStub } from "core/domain/connector";
import { useExperiment } from "core/services/Experiment";
import { useIsCloudApp } from "core/utils/app";

import styles from "./ConnectorDefinitionBadges.module.scss";

interface ConnectorDefinitionBadgesProps {
  definition: ConnectorDefinitionOrEnterpriseStub;
}

export const isAgentConnectorDefinition = (
  definition: ConnectorDefinitionOrEnterpriseStub,
  supportedSourceDefinitionIds: Set<string>,
  supportedDestinationDefinitionIds: Set<string>
): boolean => {
  if ("isEnterprise" in definition) {
    return false;
  }

  return "sourceDefinitionId" in definition
    ? supportedSourceDefinitionIds.has(definition.sourceDefinitionId)
    : supportedDestinationDefinitionIds.has(definition.destinationDefinitionId);
};

export const useShowConnectorCapabilities = (): boolean => {
  const isCloudApp = useIsCloudApp();
  const optInEnabled = useExperiment("adp.external-cloud-orgs.enabled");
  const isAdpOrganization = useIsAdpOrganization();

  return isCloudApp && (optInEnabled || isAdpOrganization);
};

const ConnectorDefinitionBadgesContent: React.FC<ConnectorDefinitionBadgesProps> = ({ definition }) => {
  const showConnectorCapabilities = useShowConnectorCapabilities();
  const supportedSourceDefinitionIds = useAgentsSupportedSourceDefinitionIds();
  const supportedDestinationDefinitionIds = useAgentsSupportedDestinationDefinitionIds();

  if (!showConnectorCapabilities) {
    return null;
  }

  const supportsAgents = isAgentConnectorDefinition(
    definition,
    supportedSourceDefinitionIds,
    supportedDestinationDefinitionIds
  );

  return (
    <FlexContainer as="span" className={styles.connectorDefinitionBadges}>
      <Badge variant="purple" radius="2xs" uppercase={false}>
        <FormattedMessage id="connector.badge.dataReplication" />
      </Badge>
      {supportsAgents && (
        <Badge variant="coral" radius="2xs" uppercase={false}>
          <FormattedMessage id="connector.badge.agent" />
        </Badge>
      )}
    </FlexContainer>
  );
};

export const ConnectorDefinitionBadges: React.FC<ConnectorDefinitionBadgesProps> = (props) => (
  <Suspense>
    <ConnectorDefinitionBadgesContent {...props} />
  </Suspense>
);
