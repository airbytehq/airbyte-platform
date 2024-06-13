import React, { Suspense } from "react";
import { useIntl } from "react-intl";
import { Outlet } from "react-router-dom";

import { LoadingPage } from "components";

import { SettingsLayout, SettingsLayoutContent } from "area/settings/components/SettingsLayout";
import { SettingsLink, SettingsNavigation, SettingsNavigationBlock } from "area/settings/components/SettingsNavigation";
import { useCurrentWorkspace, useGetInstanceConfiguration } from "core/api";
import { InstanceConfigurationResponseTrackingStrategy } from "core/api/types/AirbyteClient";
import { FeatureItem, useFeature } from "core/services/features";
import { useIntent } from "core/utils/rbac";
import { useGetConnectorsOutOfDate } from "hooks/services/useConnector";
import { SettingsRoutePaths } from "pages/routePaths";

export const SettingsPage: React.FC = () => {
  const { organizationId, workspaceId } = useCurrentWorkspace();
  const { trackingStrategy } = useGetInstanceConfiguration();
  const { countNewSourceVersion, countNewDestinationVersion } = useGetConnectorsOutOfDate();
  const multiWorkspaceUI = useFeature(FeatureItem.MultiWorkspaceUI);
  const apiTokenManagement = useFeature(FeatureItem.APITokenManagement);
  const canViewWorkspaceSettings = useIntent("ViewWorkspaceSettings", { workspaceId });
  const canViewOrganizationSettings = useIntent("ViewOrganizationSettings", { organizationId });
  const { formatMessage } = useIntl();

  return (
    <SettingsLayout>
      <SettingsNavigation>
        <SettingsNavigationBlock title={formatMessage({ id: "settings.userSettings" })}>
          <SettingsLink
            iconType="user"
            name={formatMessage({ id: "settings.account" })}
            to={SettingsRoutePaths.Account}
          />
          {apiTokenManagement && (
            <SettingsLink
              iconType="grid"
              name={formatMessage({ id: "settings.applications" })}
              to={SettingsRoutePaths.Applications}
            />
          )}
        </SettingsNavigationBlock>
        {canViewWorkspaceSettings && (
          <SettingsNavigationBlock title={formatMessage({ id: "settings.workspaceSettings" })}>
            {multiWorkspaceUI && (
              <SettingsLink
                iconType="community"
                name={formatMessage({
                  id: "settings.general",
                })}
                to={SettingsRoutePaths.Workspace}
              />
            )}
            {!multiWorkspaceUI && (
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
            {trackingStrategy === InstanceConfigurationResponseTrackingStrategy.segment && (
              <SettingsLink
                iconType="chart"
                name={formatMessage({ id: "settings.metrics" })}
                to={SettingsRoutePaths.Metrics}
              />
            )}
          </SettingsNavigationBlock>
        )}
        {multiWorkspaceUI && (canViewOrganizationSettings || canViewWorkspaceSettings) && (
          <SettingsNavigationBlock title={formatMessage({ id: "settings.organizationSettings" })}>
            {multiWorkspaceUI && canViewOrganizationSettings && (
              <SettingsLink
                iconType="community"
                name={formatMessage({ id: "settings.general" })}
                to={SettingsRoutePaths.Organization}
              />
            )}
            {multiWorkspaceUI && canViewWorkspaceSettings && (
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
