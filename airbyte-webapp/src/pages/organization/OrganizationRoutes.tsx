import React from "react";
import { Navigate, Route, Routes } from "react-router-dom";

import { useCurrentOrganizationId } from "area/organization/utils";
import { FeatureItem, useFeature } from "core/services/features";
import { Intent, useGeneratedIntent, useIntent } from "core/utils/rbac";
import { useExperiment } from "hooks/services/Experiment";
import { CloudSettingsRoutePaths } from "packages/cloud/views/settings/routePaths";
import { EmbeddedOnboardingPage } from "pages/embedded/EmbeddedOnboardingPage/EmbeddedOnboardingPage";
import { GeneralOrganizationSettingsPage } from "pages/SettingsPage/pages/Organization/GeneralOrganizationSettingsPage";
import { OrganizationMembersPage } from "pages/SettingsPage/pages/Organization/OrganizationMembersPage";

import { RoutePaths } from "../routePaths";

// Import organization-related components
const WorkspacesPage = React.lazy(() => import("pages/workspaces"));
const OrganizationBillingPage = React.lazy(() => import("packages/cloud/views/billing/OrganizationBillingPage"));
const OrganizationUsagePage = React.lazy(() => import("packages/cloud/views/billing/OrganizationUsagePage"));

export const OrganizationRoutes: React.FC = () => {
  const organizationId = useCurrentOrganizationId();
  const multiWorkspaceUI = useFeature(FeatureItem.MultiWorkspaceUI);
  const canViewOrgSettings = useIntent("ViewOrganizationSettings", { organizationId });
  const canManageOrganizationBilling = useGeneratedIntent(Intent.ManageOrganizationBilling);
  const canViewOrganizationUsage = useGeneratedIntent(Intent.ViewOrganizationUsage);
  const defaultPath = multiWorkspaceUI && canViewOrgSettings ? RoutePaths.Workspaces : RoutePaths.Settings;
  const isEmbedded = useExperiment("platform.allow-config-template-endpoints");

  return (
    <Routes>
      {isEmbedded && <Route path={RoutePaths.EmbeddedOnboarding} element={<EmbeddedOnboardingPage />} />}
      {multiWorkspaceUI && canViewOrgSettings && (
        <>
          <Route path={RoutePaths.Workspaces} element={<WorkspacesPage />} />
          <Route path={CloudSettingsRoutePaths.OrganizationMembers} element={<OrganizationMembersPage />} />
        </>
      )}
      {canManageOrganizationBilling && (
        <Route path={CloudSettingsRoutePaths.Billing} element={<OrganizationBillingPage />} />
      )}
      {canViewOrganizationUsage && (
        <Route path={CloudSettingsRoutePaths.OrganizationUsage} element={<OrganizationUsagePage />} />
      )}
      <Route path={RoutePaths.Settings} element={<GeneralOrganizationSettingsPage />} />
      <Route path="*" element={<Navigate to={defaultPath} replace />} />
    </Routes>
  );
};
