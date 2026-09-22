import React from "react";
import { FormattedMessage } from "react-intl";

import { NavItem } from "area/layout/SideBar/components/NavItem";
import { useCurrentOrganizationId } from "area/organization/utils";
import { CloudSettingsRoutePaths } from "cloud/views/settings/routePaths";
import { useAgentsProvisioningStatus } from "core/api";
import { useIsCloudApp } from "core/utils/app";
import { RoutePaths } from "pages/routePaths";

import styles from "./AgentsSidebarLink.module.scss";
import { useShowAgentsOptIn } from "./useShowAgentsOptIn";

interface AgentsSidebarLinkContentProps {
  labelId: string;
  icon: "aiStars" | "mcp";
  routePath: string;
  testId: string;
  isContextLayer?: boolean;
}

const AgentsSidebarLinkContent: React.FC<AgentsSidebarLinkContentProps> = ({
  labelId,
  icon,
  routePath,
  testId,
  isContextLayer,
}) => {
  const isCloudApp = useIsCloudApp();
  const showAgentsOptIn = useShowAgentsOptIn();
  const organizationId = useCurrentOrganizationId();
  const status = useAgentsProvisioningStatus({ enabled: isCloudApp && showAgentsOptIn });

  if (!isCloudApp || !showAgentsOptIn || !status || (!status.is_enrolled && !status.external_cloud_eligible)) {
    return null;
  }

  const href = `/${RoutePaths.Organization}/${organizationId}/${RoutePaths.Settings}/${routePath}`;

  return (
    <NavItem
      label={<FormattedMessage id={labelId} />}
      icon={icon}
      to={href}
      testId={testId}
      className={isContextLayer ? styles.contextLayerLink : styles.installMcpLink}
      labelColor={isContextLayer ? "blue" : undefined}
      withBadge={isContextLayer ? "new" : "beta"}
    />
  );
};

export const AgentsSidebarLink: React.FC = () => {
  return (
    <React.Suspense>
      <AgentsSidebarLinkContent
        labelId="cloud.contextLayer.sidebar"
        icon="aiStars"
        routePath={CloudSettingsRoutePaths.ContextLayer}
        testId="agentsSidebarLink"
        isContextLayer
      />
    </React.Suspense>
  );
};

export const InstallMcpSidebarLink: React.FC = () => {
  return (
    <React.Suspense>
      <AgentsSidebarLinkContent
        labelId="cloud.installMcp.sidebar"
        icon="mcp"
        routePath={CloudSettingsRoutePaths.InstallMcp}
        testId="installMcpSidebarLink"
      />
    </React.Suspense>
  );
};
