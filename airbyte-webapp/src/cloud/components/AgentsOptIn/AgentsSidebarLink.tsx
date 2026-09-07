import React from "react";
import { FormattedMessage } from "react-intl";

import { NavItem } from "area/layout/SideBar/components/NavItem";
import { useCurrentOrganizationId } from "area/organization/utils";
import { CloudSettingsRoutePaths } from "cloud/views/settings/routePaths";
import { useAgentsProvisioningStatus } from "core/api";
import { useIsCloudApp } from "core/utils/app";
import { RoutePaths } from "pages/routePaths";

import { useShowAgentsOptIn } from "./useShowAgentsOptIn";

const AgentsSidebarLinkContent: React.FC = () => {
  const isCloudApp = useIsCloudApp();
  const showAgentsOptIn = useShowAgentsOptIn();
  const organizationId = useCurrentOrganizationId();
  const status = useAgentsProvisioningStatus({ enabled: isCloudApp && showAgentsOptIn });

  if (!isCloudApp || !showAgentsOptIn || !status || (!status.is_enrolled && !status.external_cloud_eligible)) {
    return null;
  }

  const href = `/${RoutePaths.Organization}/${organizationId}/${RoutePaths.Settings}/${CloudSettingsRoutePaths.ContextLayer}`;

  return (
    <NavItem
      label={<FormattedMessage id="cloud.contextLayer.sidebar" />}
      icon={status.is_enrolled ? "file" : "aiStars"}
      to={href}
      testId="agentsSidebarLink"
    />
  );
};

export const AgentsSidebarLink: React.FC = () => {
  return (
    <React.Suspense>
      <AgentsSidebarLinkContent />
    </React.Suspense>
  );
};
