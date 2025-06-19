import React, { Suspense } from "react";
import { useIntl } from "react-intl";
import { Outlet } from "react-router-dom";

import { LoadingPage } from "components";

import { SettingsLayout, SettingsLayoutContent } from "area/settings/components/SettingsLayout";
import { SettingsLink, SettingsNavigation, SettingsNavigationBlock } from "area/settings/components/SettingsNavigation";
import { useCurrentWorkspace, useGetInstanceConfiguration } from "core/api";
import { InstanceConfigurationResponseTrackingStrategy } from "core/api/types/AirbyteClient";
import { useAuthService } from "core/services/auth";
import { FeatureItem, useFeature } from "core/services/features";
import { useIntent } from "core/utils/rbac";
import { useGetConnectorsOutOfDate } from "hooks/services/useConnector";
import { SettingsRoutePaths } from "pages/routePaths";

export const SettingsPage: React.FC = () => {
  const { formatMessage } = useIntl();
  const { organizationId, workspaceId } = useCurrentWorkspace();
  const { trackingStrategy } = useGetInstanceConfiguration();
  const { countNewSourceVersion, countNewDestinationVersion } = useGetConnectorsOutOfDate();
  const multiWorkspaceUI = useFeature(FeatureItem.MultiWorkspaceUI);
  const { applicationSupport } = useAuthService();
  const licenseUi = useFeature(FeatureItem.EnterpriseLicenseChecking);
  const canViewLicenseSettings = useIntent("ViewLicenseDetails", { workspaceId });
  const displayOrganizationUsers = useFeature(FeatureItem.DisplayOrganizationUsers);
  const canViewWorkspaceSettings = useIntent("ViewWorkspaceSettings", { workspaceId });
  const canViewOrganizationSettings = useIntent("ViewOrganizationSettings", { organizationId });

  const showLicenseUi = licenseUi && canViewLicenseSettings;

  return (
    <SettingsLayout>
      <SettingsNavigation>
        <SettingsNavigationBlock title={formatMessage({ id: "settings.userSettings" })}>
          <SettingsLink
            iconType="user"
            name={formatMessage({ id: "settings.account" })}
            to={SettingsRoutePaths.Account}
          />
          {applicationSupport !== "none" && (
            <SettingsLink
              iconType="grid"
              name={formatMessage({ id: "settings.applications" })}
              to={SettingsRoutePaths.Applications}
            />
          )}
        </SettingsNavigationBlock>
        <SettingsNavigationBlock title={formatMessage({ id: "settings.workspaceSettings" })}>
          <SettingsLink
            iconType="gear"
            name={formatMessage({
              id: "settings.general",
            })}
            to={SettingsRoutePaths.Workspace}
          />
          {multiWorkspaceUI && canViewWorkspaceSettings && (
            <SettingsLink
              iconType="community"
              name={formatMessage({ id: "settings.members" })}
              to={SettingsRoutePaths.WorkspaceMembers}
            />
          )}
          {canViewWorkspaceSettings && !multiWorkspaceUI && (
            <>
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
            </>
          )}
          <SettingsLink
            iconType="bell"
            name={formatMessage({ id: "settings.notifications" })}
            to={SettingsRoutePaths.Notifications}
          />
          {canViewWorkspaceSettings && trackingStrategy === InstanceConfigurationResponseTrackingStrategy.segment && (
            <SettingsLink
              iconType="chart"
              name={formatMessage({ id: "settings.metrics" })}
              to={SettingsRoutePaths.Metrics}
            />
          )}
        </SettingsNavigationBlock>
        {multiWorkspaceUI && (canViewOrganizationSettings || canViewWorkspaceSettings) && (
          <SettingsNavigationBlock title={formatMessage({ id: "settings.organization" })}>
            {canViewOrganizationSettings && (
              <>
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
              </>
            )}
            {showLicenseUi && (
              <SettingsLink
                iconType="license"
                name={formatMessage({ id: "settings.license" })}
                to={SettingsRoutePaths.License}
              />
            )}
            {canViewWorkspaceSettings && (
              <>
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
              </>
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
