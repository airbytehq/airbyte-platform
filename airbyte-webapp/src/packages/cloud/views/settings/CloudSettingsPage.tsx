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
import { useCurrentWorkspace } from "core/api";
import { FeatureItem, useFeature } from "core/services/features";
import { isOsanoActive, showOsanoDrawer } from "core/utils/dataPrivacy";
import { useIntent } from "core/utils/rbac";
import { useGeneratedIntent } from "core/utils/rbac/useGeneratedIntent";
import { useExperiment } from "hooks/services/Experiment";

import { CloudSettingsRoutePaths } from "./routePaths";

export const CloudSettingsPage: React.FC = () => {
  const { formatMessage } = useIntl();
  const supportsCloudDbtIntegration = useFeature(FeatureItem.AllowDBTCloudIntegration);
  const supportsDataResidency = useFeature(FeatureItem.AllowChangeDataGeographies);
  const workspace = useCurrentWorkspace();
  const canViewOrgSettings = useIntent("ViewOrganizationSettings", { organizationId: workspace.organizationId });
  const showAdvancedSettings = useExperiment("settings.showAdvancedSettings");
  const isOrganizationBillingPageVisible = useExperiment("billing.organizationBillingPage");
  const isWorkspaceUsagePageVisible = useExperiment("billing.workspaceUsagePage");
  const canManageOrganizationBilling = useGeneratedIntent("ManageOrganizationBilling");

  return (
    <SettingsLayout>
      <SettingsNavigation>
        <SettingsNavigationBlock title={formatMessage({ id: "settings.userSettings" })}>
          <SettingsLink
            iconType="user"
            name={formatMessage({ id: "settings.account" })}
            to={CloudSettingsRoutePaths.Account}
          />
          <SettingsLink
            iconType="grid"
            name={formatMessage({ id: "settings.applications" })}
            to={CloudSettingsRoutePaths.Applications}
          />
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

          {isWorkspaceUsagePageVisible && (
            <SettingsLink
              iconType="chart"
              name={formatMessage({ id: "settings.usage" })}
              to={CloudSettingsRoutePaths.Usage}
            />
          )}
        </SettingsNavigationBlock>
        {canViewOrgSettings && (
          <SettingsNavigationBlock title={formatMessage({ id: "settings.organizationSettings" })}>
            <SettingsLink
              iconType="gear"
              name={formatMessage({ id: "settings.general" })}
              to={CloudSettingsRoutePaths.Organization}
            />
            <SettingsLink
              iconType="community"
              name={formatMessage({ id: "settings.members" })}
              to={CloudSettingsRoutePaths.OrganizationMembers}
            />
            {isOrganizationBillingPageVisible && canManageOrganizationBilling && (
              <SettingsLink
                iconType="credits"
                name={formatMessage({ id: "sidebar.billing" })}
                to={CloudSettingsRoutePaths.Billing}
              />
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

export default CloudSettingsPage;
