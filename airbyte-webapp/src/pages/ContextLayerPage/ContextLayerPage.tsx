import React, { Suspense } from "react";
import { useIntl } from "react-intl";
import { Navigate, Outlet } from "react-router-dom";

import { LoadingPage } from "components";

import { useCurrentOrganizationId } from "area/organization/utils";
import { SettingsLayout, SettingsLayoutContent } from "area/settings/components/SettingsLayout";
import { SettingsLink, SettingsNavigation, SettingsNavigationBlock } from "area/settings/components/SettingsNavigation";
import { useAgentsProvisioningStatusQuery } from "core/api";
import { useIsCloudApp } from "core/utils/app";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
import { ContextLayerRoutePaths, RoutePaths } from "pages/routePaths";

const ContextLayerNavigation: React.FC = () => {
  const { formatMessage } = useIntl();
  const organizationId = useCurrentOrganizationId();
  const basePath = `/${RoutePaths.Organization}/${organizationId}/${RoutePaths.ContextLayer}`;

  return (
    <SettingsLayout titleId="cloud.contextLayer.navigation.title">
      <SettingsNavigation>
        <SettingsNavigationBlock title={formatMessage({ id: "cloud.contextLayer.navigation.title" })}>
          <SettingsLink iconType="gear" name={formatMessage({ id: "sidebar.settings" })} to={basePath} />
          <SettingsLink
            iconType="robot"
            name={formatMessage({ id: "cloud.contextLayer.agentAccess.pageTitle" })}
            to={`${basePath}/${ContextLayerRoutePaths.AgentAccess}`}
          />
        </SettingsNavigationBlock>
      </SettingsNavigation>
      <SettingsLayoutContent>
        <Suspense fallback={<LoadingPage />}>
          <Outlet />
        </Suspense>
      </SettingsLayoutContent>
    </SettingsLayout>
  );
};

export const ContextLayerPage: React.FC = () => {
  const organizationId = useCurrentOrganizationId();
  const isCloudApp = useIsCloudApp();
  const statusQuery = useAgentsProvisioningStatusQuery({ enabled: isCloudApp });
  const canManageOrganizationPermissions = useGeneratedIntent(Intent.UpdateOrganizationPermissions, { organizationId });

  if (statusQuery.isInitialLoading) {
    return <LoadingPage />;
  }
  if (statusQuery.data?.is_enrolled) {
    return <ContextLayerNavigation />;
  }
  if (!canManageOrganizationPermissions) {
    return <Navigate to={`/${RoutePaths.Organization}/${organizationId}/${RoutePaths.Workspaces}`} replace />;
  }
  if (statusQuery.isError) {
    return <ContextLayerNavigation />;
  }
  if (statusQuery.data?.external_cloud_eligible) {
    return <ContextLayerNavigation />;
  }
  return (
    <Suspense fallback={<LoadingPage />}>
      <Outlet />
    </Suspense>
  );
};
