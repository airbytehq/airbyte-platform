import React, { Suspense } from "react";
import { useIntl } from "react-intl";
import { Outlet } from "react-router-dom";

import { LoadingPage } from "components";

import { SettingsLayout, SettingsLayoutContent } from "area/settings/components/SettingsLayout";
import { SettingsLink, SettingsNavigation, SettingsNavigationBlock } from "area/settings/components/SettingsNavigation";
import { FeatureItem, useFeature } from "core/services/features";

import { CloudSettingsRoutePaths } from "./routePaths";

export const CloudSettingsPage: React.FC = () => {
  const { formatMessage } = useIntl();
  const supportsCloudDbtIntegration = useFeature(FeatureItem.AllowDBTCloudIntegration);

  return (
    <SettingsLayout>
      <SettingsNavigation>
        <SettingsNavigationBlock title={formatMessage({ id: "settings.workspaceSettings" })}>
          <SettingsLink
            iconType="gear"
            name={formatMessage({ id: "settings.general" })}
            to={CloudSettingsRoutePaths.Workspace}
          />
          <SettingsLink
            iconType="community"
            name={formatMessage({ id: "settings.members" })}
            to={CloudSettingsRoutePaths.WorkspaceMembers}
          />
          <SettingsLink
            iconType="source"
            name={formatMessage({ id: "tables.sources" })}
            to={CloudSettingsRoutePaths.Source}
          />
          <SettingsLink
            iconType="destination"
            name={formatMessage({ id: "tables.destinations" })}
            to={CloudSettingsRoutePaths.Destination}
          />
          {supportsCloudDbtIntegration && (
            <SettingsLink
              iconType="integrations"
              name={formatMessage({ id: "settings.integrationSettings" })}
              to={CloudSettingsRoutePaths.DbtCloud}
            />
          )}
          <SettingsLink
            iconType="bell"
            name={formatMessage({ id: "settings.notifications" })}
            to={CloudSettingsRoutePaths.Notifications}
          />

          <SettingsLink
            iconType="chart"
            name={formatMessage({ id: "settings.usage" })}
            to={CloudSettingsRoutePaths.Usage}
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

export default CloudSettingsPage;
