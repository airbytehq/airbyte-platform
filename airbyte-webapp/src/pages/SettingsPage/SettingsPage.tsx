import React, { Suspense } from "react";
import { useIntl } from "react-intl";
import { Outlet } from "react-router-dom";

import { LoadingPage } from "components";

import { SettingsLayout, SettingsLayoutContent } from "area/settings/components/SettingsLayout";
import { SettingsLink, SettingsNavigation, SettingsNavigationBlock } from "area/settings/components/SettingsNavigation";
import { useGetInstanceConfiguration } from "core/api";
import { InstanceConfigurationResponseTrackingStrategy } from "core/api/types/AirbyteClient";
import { useAuthService } from "core/services/auth";
import { FeatureItem, useFeature } from "core/services/features";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
import { useGetConnectorsOutOfDate } from "hooks/services/useConnector";
import { SettingsRoutePaths } from "pages/routePaths";

export const SettingsPage: React.FC = () => {
  const { formatMessage } = useIntl();
  const { trackingStrategy } = useGetInstanceConfiguration();
  const { countNewSourceVersion, countNewDestinationVersion } = useGetConnectorsOutOfDate();
  // FeatureItem.ShowWorkspacePicker is weirdly being used as a proxy for showing RBAC and source/destination settings
  // pages here. We should clean this up so that we use more appropriate feature items for toggling these pages.
  const showWorkspacePicker = useFeature(FeatureItem.ShowWorkspacePicker);
  const canViewWorkspaceSettings = useGeneratedIntent(Intent.ViewWorkspaceSettings);
  const { authType } = useAuthService();

  return (
    <SettingsLayout>
      <SettingsNavigation>
        {/* When auth is not enabled in OSS, the user settings link in the sidebar is not visible. We still want the user to
        be able to change their email, so this section shows up in the workspace settings instead. */}
        {authType === "none" && (
          <SettingsNavigationBlock title={formatMessage({ id: "settings.userSettings" })}>
            <SettingsLink
              iconType="user"
              name={formatMessage({ id: "settings.account" })}
              to={SettingsRoutePaths.Account}
            />
          </SettingsNavigationBlock>
        )}
        <SettingsNavigationBlock title={formatMessage({ id: "settings.workspaceSettings" })}>
          <SettingsLink
            iconType="gear"
            name={formatMessage({
              id: "settings.general",
            })}
            to={SettingsRoutePaths.Workspace}
          />
          {showWorkspacePicker && canViewWorkspaceSettings && (
            <SettingsLink
              iconType="community"
              name={formatMessage({ id: "settings.members" })}
              to={SettingsRoutePaths.WorkspaceMembers}
            />
          )}
          {canViewWorkspaceSettings && !showWorkspacePicker && (
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
      </SettingsNavigation>
      <SettingsLayoutContent>
        <Suspense fallback={<LoadingPage />}>
          <Outlet />
        </Suspense>
      </SettingsLayoutContent>
    </SettingsLayout>
  );
};
