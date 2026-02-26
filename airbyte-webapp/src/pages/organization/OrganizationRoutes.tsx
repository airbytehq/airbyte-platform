import React from "react";
import { Navigate, Route, Routes } from "react-router-dom";

import OrganizationSettingsLayout from "area/organization/OrganizationSettingsLayout";
import { useCurrentOrganizationId } from "area/organization/utils";
import { UserSettingsRoutes } from "area/settings/UserSettingsRoutes";
import { CloudSettingsRoutePaths } from "cloud/views/settings/routePaths";
import { useExperimentContext } from "core/services/Experiment";
import { FeatureItem, useFeature } from "core/services/features";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
import { OrganizationSettingsPage } from "pages/SettingsPage/OrganizationSettingsPage";
import { DestinationsPage, SourcesPage } from "pages/SettingsPage/pages/ConnectorsPage";
import { LicenseSettingsPage } from "pages/SettingsPage/pages/LicenseDetailsPage/LicenseSettingsPage";
import { GeneralOrganizationSettingsPage } from "pages/SettingsPage/pages/Organization/GeneralOrganizationSettingsPage";
import { OrganizationMembersPage } from "pages/SettingsPage/pages/Organization/OrganizationMembersPage";
import { SSOOrganizationSettingsPage } from "pages/SettingsPage/pages/Organization/SSOOrganizationSettingsPage";

import { RoutePaths, SettingsRoutePaths } from "../routePaths";

const OrganizationWorkspacesPage = React.lazy(() => import("pages/workspaces/OrganizationWorkspacesPage"));
const OrganizationBillingPage = React.lazy(() => import("cloud/views/billing/OrganizationBillingPage"));
const OrganizationUsagePage = React.lazy(() => import("cloud/views/billing/OrganizationUsagePage"));

export const OrganizationRoutes: React.FC = () => {
  const organizationId = useCurrentOrganizationId();
  const licenseUi = useFeature(FeatureItem.EnterpriseLicenseChecking);
  const supportsSSO = useFeature(FeatureItem.AllowUpdateSSOConfig);
  const canViewOrgSettings = useGeneratedIntent(Intent.ViewOrganizationSettings, { organizationId });
  const canManageOrganizationBilling = useGeneratedIntent(Intent.ManageOrganizationBilling, { organizationId });
  const canViewOrganizationUsage = useGeneratedIntent(Intent.ViewOrganizationUsage, { organizationId });

  useExperimentContext("organization", organizationId);

  return (
    <Routes>
      <Route path={`${SettingsRoutePaths.User}/*`} element={<UserSettingsRoutes />} />
      <Route element={<OrganizationSettingsLayout />}>
        <Route path={RoutePaths.Workspaces} element={<OrganizationWorkspacesPage />} />
        <Route path="*" element={<Navigate to={RoutePaths.Workspaces} replace />} />
      </Route>
      {canViewOrgSettings && (
        <Route path={`${RoutePaths.Settings}/*`} element={<OrganizationSettingsPage />}>
          <Route path={SettingsRoutePaths.Organization} element={<GeneralOrganizationSettingsPage />} />
          {canViewOrgSettings && (
            <Route path={SettingsRoutePaths.OrganizationMembers} element={<OrganizationMembersPage />} />
          )}
          {supportsSSO && <Route path={SettingsRoutePaths.OrganizationSSO} element={<SSOOrganizationSettingsPage />} />}
          {licenseUi && <Route path={SettingsRoutePaths.License} element={<LicenseSettingsPage />} />}
          {canManageOrganizationBilling && (
            <Route path={CloudSettingsRoutePaths.Billing} element={<OrganizationBillingPage />} />
          )}
          {canViewOrganizationUsage && (
            <Route path={CloudSettingsRoutePaths.OrganizationUsage} element={<OrganizationUsagePage />} />
          )}
          <Route path={SettingsRoutePaths.Source} element={<SourcesPage />} />
          <Route path={SettingsRoutePaths.Destination} element={<DestinationsPage />} />
          <Route path="*" element={<Navigate to={SettingsRoutePaths.Organization} replace />} />
        </Route>
      )}
    </Routes>
  );
};
