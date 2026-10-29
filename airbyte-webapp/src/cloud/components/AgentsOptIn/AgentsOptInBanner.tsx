import React from "react";
import { FormattedMessage } from "react-intl";

import { Link } from "components/ui/Link";

import { useCurrentOrganizationId } from "area/organization/utils";
import { HideableBanner } from "cloud/components/CloudBannerGroup/components/HideableBanner";
import { useAgentsProvisioningStatus } from "core/api";
import { RoutePaths } from "pages/routePaths";

import { useShowAgentsOptIn } from "./useShowAgentsOptIn";

const AgentsOptInBannerContent: React.FC = () => {
  const showAgentsOptIn = useShowAgentsOptIn();
  const organizationId = useCurrentOrganizationId();
  const status = useAgentsProvisioningStatus({ enabled: showAgentsOptIn });

  if (!showAgentsOptIn || !status || status.is_enrolled || !status.external_cloud_eligible) {
    return null;
  }

  const settingsUrl = `/${RoutePaths.Organization}/${organizationId}/${RoutePaths.ContextLayer}`;

  return (
    <HideableBanner
      id="agents-opt-in"
      data-testid="agents-opt-in-banner"
      message={
        <FormattedMessage
          id="cloud.agentsOptIn.banner"
          values={{
            lnk: (node: React.ReactNode) => <Link to={settingsUrl}>{node}</Link>,
          }}
        />
      }
    />
  );
};

export const AgentsOptInBanner: React.FC = () => {
  return (
    <React.Suspense>
      <AgentsOptInBannerContent />
    </React.Suspense>
  );
};
