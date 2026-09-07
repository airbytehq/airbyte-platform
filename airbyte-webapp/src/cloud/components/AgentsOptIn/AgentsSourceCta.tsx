import React from "react";
import { FormattedMessage } from "react-intl";
import { useNavigate } from "react-router-dom";

import { Button } from "components/ui/Button";

import { useCurrentOrganizationId } from "area/organization/utils";
import { CloudSettingsRoutePaths } from "cloud/views/settings/routePaths";
import { useAgentsProvisioningStatus, useAgentsSupportedSourceDefinitions } from "core/api";
import { useIsCloudApp } from "core/utils/app";
import { links } from "core/utils/links";
import { RoutePaths } from "pages/routePaths";

import { useShowAgentsOptIn } from "./useShowAgentsOptIn";

interface AgentsSourceCtaProps {
  actorType: "source" | "destination";
  actorDefinitionName: string;
}

const AgentsSourceCtaContent: React.FC<AgentsSourceCtaProps> = ({ actorType, actorDefinitionName }) => {
  const isCloudApp = useIsCloudApp();
  const showAgentsOptIn = useShowAgentsOptIn();
  const supportedSourceDefinitions = useAgentsSupportedSourceDefinitions();
  const status = useAgentsProvisioningStatus({ enabled: isCloudApp && showAgentsOptIn });
  const organizationId = useCurrentOrganizationId();
  const navigate = useNavigate();

  if (
    !isCloudApp ||
    !showAgentsOptIn ||
    !status ||
    (!status.is_enrolled && !status.external_cloud_eligible) ||
    actorType !== "source" ||
    !supportedSourceDefinitions.has(actorDefinitionName)
  ) {
    return null;
  }

  const handleClick = (event: React.MouseEvent<HTMLButtonElement>) => {
    event.preventDefault();
    event.stopPropagation();

    if (status?.is_enrolled) {
      window.open(
        `${links.agentEngineApp}/organizations/${organizationId}/get-started`,
        "_blank",
        "noopener,noreferrer"
      );
      return;
    }

    navigate(
      `/${RoutePaths.Organization}/${organizationId}/${RoutePaths.Settings}/${CloudSettingsRoutePaths.ContextLayer}`
    );
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
