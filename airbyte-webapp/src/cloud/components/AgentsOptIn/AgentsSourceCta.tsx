import React from "react";
import { FormattedMessage } from "react-intl";
import { useNavigate } from "react-router-dom";

import { Button } from "components/ui/Button";

import { useCurrentOrganizationId } from "area/organization/utils";
import { useAgentsProvisioningStatus, useAgentsSupportedSourceDefinitionIds } from "core/api";
import { useIsCloudApp } from "core/utils/app";
import { RoutePaths } from "pages/routePaths";

import { useShowAgentsOptIn } from "./useShowAgentsOptIn";

interface AgentsSourceCtaProps {
  actorType: "source" | "destination";
  actorDefinitionId: string;
}

const AgentsSourceCtaContent: React.FC<AgentsSourceCtaProps> = ({ actorType, actorDefinitionId }) => {
  const isCloudApp = useIsCloudApp();
  const showAgentsOptIn = useShowAgentsOptIn();
  const supportedSourceDefinitionIds = useAgentsSupportedSourceDefinitionIds();
  const status = useAgentsProvisioningStatus({ enabled: isCloudApp && showAgentsOptIn });
  const organizationId = useCurrentOrganizationId();
  const navigate = useNavigate();

  if (
    !isCloudApp ||
    !showAgentsOptIn ||
    !status ||
    (!status.is_enrolled && !status.external_cloud_eligible) ||
    actorType !== "source" ||
    !supportedSourceDefinitionIds.has(actorDefinitionId)
  ) {
    return null;
  }

  const handleClick = (event: React.MouseEvent<HTMLButtonElement>) => {
    event.preventDefault();
    event.stopPropagation();

    navigate(`/${RoutePaths.Organization}/${organizationId}/${RoutePaths.ContextLayer}`);
  };

  return (
    <Button type="button" variant="secondary" size="xs" icon="aiStars" onClick={handleClick}>
      <FormattedMessage id="cloud.agentsOptIn.tryWithAgents" />
    </Button>
  );
};

export const AgentsSourceCta: React.FC<AgentsSourceCtaProps> = (props) => (
  <React.Suspense>
    <AgentsSourceCtaContent {...props} />
  </React.Suspense>
);
