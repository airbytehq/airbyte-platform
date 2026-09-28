import React from "react";
import { FormattedMessage } from "react-intl";

import { NavItem } from "area/layout/SideBar/components/NavItem";
import { useCurrentOrganizationId } from "area/organization/utils";
import { CloudSettingsRoutePaths } from "cloud/views/settings/routePaths";
import { useAgentsProvisioningStatus } from "core/api";
import { useIsCloudApp } from "core/utils/app";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
import { RoutePaths } from "pages/routePaths";

import styles from "./AgentsSidebarLink.module.scss";
import { useShowAgentsOptIn } from "./useShowAgentsOptIn";

const UnenrolledContextLayerSidebarLink: React.FC<{ organizationId: string }> = ({ organizationId }) => {
  const canManageOrganizationPermissions = useGeneratedIntent(Intent.UpdateOrganizationPermissions, { organizationId });

  if (!canManageOrganizationPermissions) {
    return null;
  }

  return (
    <NavItem
      label={<FormattedMessage id="cloud.contextLayer.sidebar" />}
      icon="aiStars"
      to={`/${RoutePaths.Organization}/${organizationId}/${RoutePaths.ContextLayer}`}
      testId="agentsSidebarLink"
      className={styles.contextLayerLink}
      labelColor="blue"
      withBadge="new"
    />
  );
};

const ContextLayerSidebarLink: React.FC = () => {
  const isCloudApp = useIsCloudApp();
  const showAgentsOptIn = useShowAgentsOptIn();
  const organizationId = useCurrentOrganizationId();
  const status = useAgentsProvisioningStatus({ enabled: isCloudApp });

  if (!isCloudApp || !status) {
    return null;
  }
  if (!status.is_enrolled) {
    return status.external_cloud_eligible && showAgentsOptIn ? (
      <UnenrolledContextLayerSidebarLink organizationId={organizationId} />
    ) : null;
  }

  return (
    <NavItem
      label={<FormattedMessage id="cloud.contextLayer.sidebar" />}
      icon="aiStars"
      to={`/${RoutePaths.Organization}/${organizationId}/${RoutePaths.ContextLayer}`}
      testId="agentsSidebarLink"
      className={styles.contextLayerLink}
      labelColor="blue"
      withBadge="new"
    />
  );
};

const InstallMcpSidebarLinkContent: React.FC = () => {
  const isCloudApp = useIsCloudApp();
  const showAgentsOptIn = useShowAgentsOptIn();
  const organizationId = useCurrentOrganizationId();
  const status = useAgentsProvisioningStatus({ enabled: isCloudApp && showAgentsOptIn });

  if (!isCloudApp || !showAgentsOptIn || !status || (!status.is_enrolled && !status.external_cloud_eligible)) {
    return null;
  }

  return (
    <NavItem
      label={<FormattedMessage id="cloud.installMcp.sidebar" />}
      icon="mcp"
      to={`/${RoutePaths.Organization}/${organizationId}/${CloudSettingsRoutePaths.InstallMcp}`}
      testId="installMcpSidebarLink"
      className={styles.installMcpLink}
      withBadge="beta"
    />
  );
};

export const AgentsSidebarLink: React.FC = () => {
  return (
    <React.Suspense>
      <ContextLayerSidebarLink />
    </React.Suspense>
  );
};

export const InstallMcpSidebarLink: React.FC = () => {
  return (
    <React.Suspense>
      <InstallMcpSidebarLinkContent />
    </React.Suspense>
  );
};
