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
import { Intent, useIntent, useGeneratedIntent } from "core/utils/rbac";
import { useExperiment } from "hooks/services/Experiment";
import { useShowBillingPageV2 } from "packages/cloud/area/billing/utils/useShowBillingPage";

import { CloudSettingsRoutePaths } from "./routePaths";

export const CloudSettingsPage: React.FC = () => {
  const { formatMessage } = useIntl();
  const supportsCloudDbtIntegration = useFeature(FeatureItem.AllowDBTCloudIntegration);
  const supportsDataResidency = useFeature(FeatureItem.AllowChangeDataGeographies);
  const workspace = useCurrentWorkspace();
  const canViewOrgSettings = useIntent("ViewOrganizationSettings", { organizationId: workspace.organizationId });
  const showAdvancedSettings = useExperiment("settings.showAdvancedSettings");
  const showBillingPageV2 = useShowBillingPageV2();
  const canManageOrganizationBilling = useGeneratedIntent(Intent.ManageOrganizationBilling);
  const canViewOrganizationUsage = useGeneratedIntent(Intent.ViewOrganizationUsage);

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

          {showBillingPageV2 && (
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
            {showBillingPageV2 && canManageOrganizationBilling && (
              <SettingsLink
                iconType="credits"
                name={formatMessage({ id: "sidebar.billing" })}
                to={CloudSettingsRoutePaths.Billing}
              />
            )}
            {showBillingPageV2 && canViewOrganizationUsage && (
              <SettingsLink
                iconType="chart"
                name={formatMessage({ id: "settings.usage" })}
                to={CloudSettingsRoutePaths.OrganizationUsage}
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
