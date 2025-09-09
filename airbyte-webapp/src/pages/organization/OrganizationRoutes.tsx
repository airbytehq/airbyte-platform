import React from "react";
import { Navigate, Route, Routes } from "react-router-dom";

import OrganizationSettingsLayout from "area/organization/OrganizationSettingsLayout";
import { useCurrentOrganizationId } from "area/organization/utils";
import { UserSettingsRoutes } from "area/settings/UserSettingsRoutes";
import { FeatureItem, useFeature } from "core/services/features";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
import { useExperiment, useExperimentContext } from "hooks/services/Experiment";
import { CloudSettingsRoutePaths } from "packages/cloud/views/settings/routePaths";
import { EmbeddedOnboardingPage } from "pages/embedded/EmbeddedOnboardingPage/EmbeddedOnboardingPage";
import { OrganizationSettingsPage } from "pages/SettingsPage/OrganizationSettingsPage";
import { DestinationsPage, SourcesPage } from "pages/SettingsPage/pages/ConnectorsPage";
import { LicenseSettingsPage } from "pages/SettingsPage/pages/LicenseDetailsPage/LicenseSettingsPage";
import { GeneralOrganizationSettingsPage } from "pages/SettingsPage/pages/Organization/GeneralOrganizationSettingsPage";
import { OrganizationMembersPage } from "pages/SettingsPage/pages/Organization/OrganizationMembersPage";

import { RoutePaths, SettingsRoutePaths } from "../routePaths";

const OrganizationWorkspacesPage = React.lazy(() => import("pages/workspaces/OrganizationWorkspacesPage"));
const OrganizationBillingPage = React.lazy(() => import("packages/cloud/views/billing/OrganizationBillingPage"));
const OrganizationUsagePage = React.lazy(() => import("packages/cloud/views/billing/OrganizationUsagePage"));

export const OrganizationRoutes: React.FC = () => {
  const organizationId = useCurrentOrganizationId();
  const licenseUi = useFeature(FeatureItem.EnterpriseLicenseChecking);
  const canViewOrgSettings = useGeneratedIntent(Intent.ViewOrganizationSettings, { organizationId });
  const canManageOrganizationBilling = useGeneratedIntent(Intent.ManageOrganizationBilling, { organizationId });
  const canViewOrganizationUsage = useGeneratedIntent(Intent.ViewOrganizationUsage, { organizationId });
  const isEmbedded = useExperiment("platform.allow-config-template-endpoints");

  useExperimentContext("organization", organizationId);

  return (
    <Routes>
      {isEmbedded && <Route path={RoutePaths.EmbeddedOnboarding} element={<EmbeddedOnboardingPage />} />}
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
