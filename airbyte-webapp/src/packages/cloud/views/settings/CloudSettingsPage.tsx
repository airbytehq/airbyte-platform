import React, { Suspense } from "react";
import { useIntl } from "react-intl";
import { Outlet } from "react-router-dom";

import { LoadingPage } from "components";

import { SettingsLayout, SettingsLayoutContent } from "area/settings/components/SettingsLayout";
import {
  SettingsButton,
  SettingsLink,
  SettingsNavigation,
  SettingsNavigationBlock,
} from "area/settings/components/SettingsNavigation";
import { useCurrentOrganizationInfo } from "core/api";
import { FeatureItem, useFeature } from "core/services/features";
import { isOsanoActive, showOsanoDrawer } from "core/utils/dataPrivacy";
import { useIntent } from "core/utils/rbac";
import { useExperiment } from "hooks/services/Experiment";

import { CloudSettingsRoutePaths } from "./routePaths";

export const CloudSettingsPage: React.FC = () => {
  const { formatMessage } = useIntl();
  const supportsCloudDbtIntegration = useFeature(FeatureItem.AllowDBTCloudIntegration);
  const supportsDataResidency = useFeature(FeatureItem.AllowChangeDataGeographies);
  const isTokenManagementEnabled = useExperiment("settings.token-management-ui", false);
  const organization = useCurrentOrganizationInfo();
  const canViewOrgSettings = useIntent("ViewOrganizationSettings", { organizationId: organization?.organizationId });
  const showAdvancedSettings = useExperiment("settings.showAdvancedSettings", false);

  return (
    <SettingsLayout>
      <SettingsNavigation>
        <SettingsNavigationBlock title={formatMessage({ id: "settings.userSettings" })}>
          <SettingsLink
            iconType="user"
            name={formatMessage({ id: "settings.account" })}
            to={CloudSettingsRoutePaths.Account}
          />
          {isTokenManagementEnabled && (
            <SettingsLink
              iconType="grid"
              name={formatMessage({ id: "settings.applications" })}
              to={CloudSettingsRoutePaths.Applications}
            />
          )}
          {isOsanoActive() && (
            <SettingsButton
              iconType="parameters"
              onClick={() => showOsanoDrawer()}
              name={formatMessage({ id: "settings.cookiePreferences" })}
            />
          )}
          {showAdvancedSettings && (
            <SettingsLink
              iconType="gear"
              name={formatMessage({ id: "settings.advanced" })}
              to={CloudSettingsRoutePaths.Advanced}
            />
          )}
        </SettingsNavigationBlock>
        <SettingsNavigationBlock title={formatMessage({ id: "settings.workspaceSettings" })}>
          <SettingsLink
            iconType="community"
            name={formatMessage({ id: "settings.general" })}
            to={CloudSettingsRoutePaths.Workspace}
          />
          {supportsDataResidency && (
            <SettingsLink
              iconType="globe"
              name={formatMessage({ id: "settings.dataResidency" })}
              to={CloudSettingsRoutePaths.DataResidency}
            />
          )}
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
        </SettingsNavigationBlock>
        {organization && canViewOrgSettings && (
          <SettingsNavigationBlock title={formatMessage({ id: "settings.organizationSettings" })}>
            <SettingsLink
              iconType="community"
              name={formatMessage({ id: "settings.general" })}
              to={CloudSettingsRoutePaths.Organization}
            />
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

export default CloudSettingsPage;
