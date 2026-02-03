import React, { Suspense } from "react";
import { useIntl } from "react-intl";
import { Outlet } from "react-router-dom";

import { LoadingPage } from "components";

import { useGetConnectorsOutOfDate } from "area/connector/utils/useConnector";
import { useCurrentOrganizationId } from "area/organization/utils";
import { SettingsLayout, SettingsLayoutContent } from "area/settings/components/SettingsLayout";
import { SettingsLink, SettingsNavigation, SettingsNavigationBlock } from "area/settings/components/SettingsNavigation";
import { CloudSettingsRoutePaths } from "cloud/views/settings/routePaths";
import { useDefaultWorkspaceInOrganization } from "core/api";
import { FeatureItem, IfFeatureEnabled, useFeature } from "core/services/features";
import { useIsCloudApp } from "core/utils/app";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
import { SettingsRoutePaths } from "pages/routePaths";

export const OrganizationSettingsPage: React.FC = () => {
  const { formatMessage } = useIntl();
  const organizationId = useCurrentOrganizationId();
  const displayOrganizationUsers = useFeature(FeatureItem.DisplayOrganizationUsers);
  const canUpdateSSOConfig = useFeature(FeatureItem.AllowUpdateSSOConfig);
  const canViewOrganizationSettings = useGeneratedIntent(Intent.ViewOrganizationSettings, { organizationId });
  const canManageOrganizationBilling = useGeneratedIntent(Intent.ManageOrganizationBilling, { organizationId });
  const canViewOrganizationUsage = useGeneratedIntent(Intent.ViewOrganizationUsage, { organizationId });
  const licenseUi = useFeature(FeatureItem.EnterpriseLicenseChecking);
  const { countNewSourceVersion, countNewDestinationVersion } = useGetConnectorsOutOfDate();

  const defaultWorkspace = useDefaultWorkspaceInOrganization(organizationId);
  const isCloudApp = useIsCloudApp();

  return (
    <SettingsLayout>
      <SettingsNavigation>
        {canViewOrganizationSettings && (
          <SettingsNavigationBlock title={formatMessage({ id: "settings.organization" })}>
            <SettingsLink
              iconType="gear"
              name={formatMessage({ id: "settings.general" })}
              to={SettingsRoutePaths.Organization}
            />
            {displayOrganizationUsers && (
              <SettingsLink
                iconType="community"
                name={formatMessage({ id: "settings.members" })}
                to={SettingsRoutePaths.OrganizationMembers}
              />
            )}
            {isCloudApp && canManageOrganizationBilling && (
              <SettingsLink
                iconType="credits"
                name={formatMessage({ id: "sidebar.billing" })}
                to={CloudSettingsRoutePaths.Billing}
              />
            )}
            {isCloudApp && canViewOrganizationUsage && (
              <SettingsLink
                iconType="chart"
                name={formatMessage({ id: "settings.usage" })}
                to={CloudSettingsRoutePaths.OrganizationUsage}
              />
            )}
            {canUpdateSSOConfig && (
              <SettingsLink
                iconType="lock"
                name={formatMessage({ id: "settings.sso" })}
                to={SettingsRoutePaths.OrganizationSSO}
              />
            )}
            {licenseUi && (
              <SettingsLink
                iconType="license"
                name={formatMessage({ id: "settings.license" })}
                to={SettingsRoutePaths.License}
              />
            )}
            {defaultWorkspace && (
              <IfFeatureEnabled feature={FeatureItem.OrganizationConnectorSettings}>
                <SettingsLink
                  iconType="source"
                  count={countNewSourceVersion}
                  name={formatMessage({ id: "tables.sources" })}
                  to={SettingsRoutePaths.Source}
                />
                <SettingsLink
                  iconType="destination"
                  count={countNewDestinationVersion}
                  name={formatMessage({ id: "tables.destinations" })}
                  to={SettingsRoutePaths.Destination}
                />
              </IfFeatureEnabled>
            )}
          </SettingsNavigationBlock>
        )}
      </SettingsNavigation>
      <SettingsLayoutContent>
        <Suspense fallback={<LoadingPage />}>
          <Outlet />
        </Suspense>
      </SettingsLayoutContent>
    </SettingsLayout>
  );
};
