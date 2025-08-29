import React, { Suspense } from "react";
import { useIntl } from "react-intl";
import { Outlet } from "react-router-dom";

import { LoadingPage } from "components";

import { useCurrentOrganizationId } from "area/organization/utils";
import { SettingsLayout, SettingsLayoutContent } from "area/settings/components/SettingsLayout";
import { SettingsLink, SettingsNavigation, SettingsNavigationBlock } from "area/settings/components/SettingsNavigation";
import { FeatureItem, useFeature } from "core/services/features";
import { useIsCloudApp } from "core/utils/app";
import { Intent, useGeneratedIntent, useIntent } from "core/utils/rbac";
import { useExperiment } from "hooks/services/Experiment";
import { CloudSettingsRoutePaths } from "packages/cloud/views/settings/routePaths";
import { RoutePaths, SettingsRoutePaths } from "pages/routePaths";

export const OrganizationSettingsPage: React.FC = () => {
  const { formatMessage } = useIntl();
  const organizationId = useCurrentOrganizationId();
  const multiWorkspaceUI = useFeature(FeatureItem.MultiWorkspaceUI);
  const displayOrganizationUsers = useFeature(FeatureItem.DisplayOrganizationUsers);
  const canViewOrganizationSettings = useIntent("ViewOrganizationSettings", { organizationId });
  const canManageOrganizationBilling = useGeneratedIntent(Intent.ManageOrganizationBilling, { organizationId });
  const canViewOrganizationUsage = useGeneratedIntent(Intent.ViewOrganizationUsage, { organizationId });
  const canManageEmbedded = useIntent("CreateConfigTemplate", { organizationId });
  const allowConfigTemplateEndpoints = useExperiment("platform.allow-config-template-endpoints");
  const isCloudApp = useIsCloudApp();

  return (
    <SettingsLayout>
      <SettingsNavigation>
        <SettingsNavigationBlock title={formatMessage({ id: "settings.organization" })}>
          {canViewOrganizationSettings && (
            <SettingsLink
              iconType="gear"
              name={formatMessage({ id: "settings.general" })}
              to={SettingsRoutePaths.Organization}
            />
          )}
          {((multiWorkspaceUI && canViewOrganizationSettings && displayOrganizationUsers) ||
            (isCloudApp && canViewOrganizationSettings)) && (
            <SettingsLink
              iconType="community"
              name={formatMessage({ id: "settings.members" })}
              to={SettingsRoutePaths.OrganizationMembers}
            />
          )}
          {isCloudApp && canViewOrganizationSettings && canManageOrganizationBilling && (
            <SettingsLink
              iconType="credits"
              name={formatMessage({ id: "sidebar.billing" })}
              to={CloudSettingsRoutePaths.Billing}
            />
          )}
          {isCloudApp && canViewOrganizationSettings && canViewOrganizationUsage && (
            <SettingsLink
              iconType="chart"
              name={formatMessage({ id: "settings.usage" })}
              to={CloudSettingsRoutePaths.OrganizationUsage}
            />
          )}
          {isCloudApp && canManageEmbedded && allowConfigTemplateEndpoints && (
            <SettingsLink
              iconType="stars"
              name={formatMessage({ id: "settings.embedded" })}
              to={`${RoutePaths.Settings}/${CloudSettingsRoutePaths.Embedded}`}
            />
          )}
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
