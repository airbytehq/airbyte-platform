import React from "react";
import { FormattedMessage } from "react-intl";

import { Badge } from "components/ui/Badge";

import { useAgentsSupportedDestinationDefinitionIds, useAgentsSupportedSourceDefinitions } from "core/api";
import { ConnectorDefinitionOrEnterpriseStub } from "core/domain/connector";
import { useIsCloudApp } from "core/utils/app";

import { useShowAgentsOptIn } from "./useShowAgentsOptIn";

interface ContextLayerDefinitionBadgeProps {
  definition: ConnectorDefinitionOrEnterpriseStub;
}

const ContextLayerDefinitionBadgeContent: React.FC<ContextLayerDefinitionBadgeProps> = ({ definition }) => {
  const isCloudApp = useIsCloudApp();
  const showAgentsOptIn = useShowAgentsOptIn();
  const supportedSourceDefinitions = useAgentsSupportedSourceDefinitions();
  const supportedDestinationDefinitionIds = useAgentsSupportedDestinationDefinitionIds();

  if (!isCloudApp || !showAgentsOptIn || "isEnterprise" in definition) {
    return null;
  }

  const supported =
    "sourceDefinitionId" in definition
      ? supportedSourceDefinitions.has(definition.name)
      : supportedDestinationDefinitionIds.has(definition.destinationDefinitionId);

  if (!supported) {
    return null;
  }

  return (
    <Badge variant="grey" uppercase={false}>
      <FormattedMessage id="cloud.contextLayer.badge" />
    </Badge>
  );
};

export const ContextLayerDefinitionBadge: React.FC<ContextLayerDefinitionBadgeProps> = (props) => (
  <React.Suspense>
    <ContextLayerDefinitionBadgeContent {...props} />
  </React.Suspense>
);
