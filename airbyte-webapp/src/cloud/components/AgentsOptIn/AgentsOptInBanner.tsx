import React from "react";
import { FormattedMessage } from "react-intl";

import { AlertBanner } from "components/ui/Banner/AlertBanner";
import { Link } from "components/ui/Link";

import { useCurrentOrganizationId } from "area/organization/utils";
import { CloudSettingsRoutePaths } from "cloud/views/settings/routePaths";
import { useAgentsProvisioningStatus } from "core/api";
import { useIsCloudApp } from "core/utils/app";
import { RoutePaths } from "pages/routePaths";

import { useShowAgentsOptIn } from "./useShowAgentsOptIn";

const AgentsOptInBannerContent: React.FC = () => {
  const isCloudApp = useIsCloudApp();
  const showAgentsOptIn = useShowAgentsOptIn();
  const organizationId = useCurrentOrganizationId();
  const status = useAgentsProvisioningStatus({ enabled: isCloudApp && showAgentsOptIn });

  if (!isCloudApp || !showAgentsOptIn || !status || status.is_enrolled || !status.external_cloud_eligible) {
    return null;
  }

  const settingsUrl = `/${RoutePaths.Organization}/${organizationId}/${RoutePaths.Settings}/${CloudSettingsRoutePaths.ContextLayer}`;

  return (
    <AlertBanner
      data-testid="agents-opt-in-banner"
      color="info"
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
