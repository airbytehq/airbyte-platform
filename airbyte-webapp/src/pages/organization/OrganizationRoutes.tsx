import React from "react";
import { Navigate, Route, Routes } from "react-router-dom";

import OrganizationSettingsLayout from "area/organization/OrganizationSettingsLayout";
import { useCurrentOrganizationId } from "area/organization/utils";
import { UserSettingsRoutes } from "area/settings/UserSettingsRoutes";
import { FeatureItem, useFeature } from "core/services/features";
import { Intent, useGeneratedIntent, useIntent } from "core/utils/rbac";
import { useExperiment } from "hooks/services/Experiment";
import { CloudSettingsRoutePaths } from "packages/cloud/views/settings/routePaths";
import { EmbeddedOnboardingPage } from "pages/embedded/EmbeddedOnboardingPage/EmbeddedOnboardingPage";
import { GeneralOrganizationSettingsPage } from "pages/SettingsPage/pages/Organization/GeneralOrganizationSettingsPage";
import { OrganizationMembersPage } from "pages/SettingsPage/pages/Organization/OrganizationMembersPage";

import { RoutePaths, SettingsRoutePaths } from "../routePaths";

const OrganizationWorkspacesPage = React.lazy(() => import("pages/workspaces/OrganizationWorkspacesPage"));
const OrganizationBillingPage = React.lazy(() => import("packages/cloud/views/billing/OrganizationBillingPage"));
const OrganizationUsagePage = React.lazy(() => import("packages/cloud/views/billing/OrganizationUsagePage"));
const EmbeddedSettingsPage = React.lazy(() =>
  import("pages/SettingsPage/pages/EmbbededSettingsPage/EmbeddedSettingsPage").then((module) => ({
    default: module.EmbeddedSettingsPage,
  }))
);

export const OrganizationRoutes: React.FC = () => {
  const organizationId = useCurrentOrganizationId();
  const multiWorkspaceUI = useFeature(FeatureItem.MultiWorkspaceUI);
  const canViewOrgSettings = useIntent("ViewOrganizationSettings", { organizationId });
  const canManageOrganizationBilling = useGeneratedIntent(Intent.ManageOrganizationBilling, { organizationId });
  const canViewOrganizationUsage = useGeneratedIntent(Intent.ViewOrganizationUsage, { organizationId });
  const defaultPath = multiWorkspaceUI && canViewOrgSettings ? RoutePaths.Workspaces : RoutePaths.Settings;
  const isEmbedded = useExperiment("platform.allow-config-template-endpoints");

  return (
    <Routes>
      {isEmbedded && <Route path={RoutePaths.EmbeddedOnboarding} element={<EmbeddedOnboardingPage />} />}
      <Route path={`${SettingsRoutePaths.User}/*`} element={<UserSettingsRoutes />} />
      <Route element={<OrganizationSettingsLayout />}>
        {multiWorkspaceUI && canViewOrgSettings && (
          <>
            <Route path={RoutePaths.Workspaces} element={<OrganizationWorkspacesPage />} />
            <Route path={CloudSettingsRoutePaths.OrganizationMembers} element={<OrganizationMembersPage />} />
          </>
        )}
        {canManageOrganizationBilling && (
          <Route path={CloudSettingsRoutePaths.Billing} element={<OrganizationBillingPage />} />
        )}
        {canViewOrganizationUsage && (
          <Route path={CloudSettingsRoutePaths.OrganizationUsage} element={<OrganizationUsagePage />} />
        )}
        <Route
          path={`${RoutePaths.Settings}/${CloudSettingsRoutePaths.Organization}`}
          element={<GeneralOrganizationSettingsPage />}
        />
        <Route path={`${RoutePaths.Settings}/${CloudSettingsRoutePaths.Embedded}`} element={<EmbeddedSettingsPage />} />
        <Route path="*" element={<Navigate to={defaultPath} replace />} />
      </Route>
    </Routes>
  );
};
