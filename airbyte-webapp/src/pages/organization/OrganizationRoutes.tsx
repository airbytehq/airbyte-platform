import React from "react";
import { Navigate, Route, Routes } from "react-router-dom";

import OrganizationSettingsLayout from "area/organization/OrganizationSettingsLayout";
import { useCurrentOrganizationId } from "area/organization/utils";
import { UserSettingsRoutes } from "area/settings/UserSettingsRoutes";
import { CloudSettingsRoutePaths } from "cloud/views/settings/routePaths";
import { useExperiment } from "core/services/Experiment";
import { FeatureItem, useFeature } from "core/services/features";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
import { OrganizationSettingsPage } from "pages/SettingsPage/OrganizationSettingsPage";
import { DestinationsPage, SourcesPage } from "pages/SettingsPage/pages/ConnectorsPage";
import { LicenseSettingsPage } from "pages/SettingsPage/pages/LicenseDetailsPage/LicenseSettingsPage";
import { GeneralOrganizationSettingsPage } from "pages/SettingsPage/pages/Organization/GeneralOrganizationSettingsPage";
import { OrganizationAuditLogsPage } from "pages/SettingsPage/pages/Organization/OrganizationAuditLogsPage";
import { OrganizationGroupsPage } from "pages/SettingsPage/pages/Organization/OrganizationGroupsPage";
import { OrganizationMembersPage } from "pages/SettingsPage/pages/Organization/OrganizationMembersPage";
import { SSOAndScimOrganizationSettingsPage } from "pages/SettingsPage/pages/Organization/SSOAndScimOrganizationSettingsPage";

import { RoutePaths, SettingsRoutePaths } from "../routePaths";

const OrganizationWorkspacesPage = React.lazy(() => import("pages/workspaces/OrganizationWorkspacesPage"));
const OrganizationBillingPage = React.lazy(() => import("cloud/views/billing/OrganizationBillingPage"));
const OrganizationPlanPage = React.lazy(() => import("cloud/views/billing/OrganizationPlanPage"));
const OrganizationUsagePage = React.lazy(() => import("cloud/views/billing/OrganizationUsagePage"));

export const OrganizationRoutes: React.FC = () => {
  const organizationId = useCurrentOrganizationId();
  const licenseUi = useFeature(FeatureItem.EnterpriseLicenseChecking);
  const supportsSSO = useFeature(FeatureItem.AllowUpdateSSOConfig);
  const auditLogsEntitled = useFeature(FeatureItem.AllowAuditLogs);
  const canViewOrgSettings = useGeneratedIntent(Intent.ViewOrganizationSettings, { organizationId });
  const canManageOrganizationBilling = useGeneratedIntent(Intent.ManageOrganizationBilling, { organizationId });
  const canViewOrganizationUsage = useGeneratedIntent(Intent.ViewOrganizationUsage, { organizationId });
  const isSelfServePlusPlanEnabled = useExperiment("billing.selfServePlusPlan");
  const isScimProvisioningEnabled = useExperiment("settings.scimProvisioning");
  const isAuditLogsUiEnabled = useExperiment("audit-log-ui");
  // UpdateOrganizationPermissions is the generated intent whose allow-list
  // (organization_admin, instance_admin) exactly matches the ORGANIZATION_ADMIN
  // security on all eight group endpoints. No group-specific intent exists.
  const canManageOrganizationPermissions = useGeneratedIntent(Intent.UpdateOrganizationPermissions, {
    organizationId,
  });

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
          {isScimProvisioningEnabled && canManageOrganizationPermissions && (
            <Route path={SettingsRoutePaths.OrganizationGroups} element={<OrganizationGroupsPage />} />
          )}
          {supportsSSO && (
            <Route path={SettingsRoutePaths.OrganizationSSO} element={<SSOAndScimOrganizationSettingsPage />} />
          )}
          {auditLogsEntitled && isAuditLogsUiEnabled && canManageOrganizationPermissions && (
            <Route path={SettingsRoutePaths.OrganizationAuditLogs} element={<OrganizationAuditLogsPage />} />
          )}
          {licenseUi && <Route path={SettingsRoutePaths.License} element={<LicenseSettingsPage />} />}
          {canManageOrganizationBilling && (
            <Route path={CloudSettingsRoutePaths.Billing} element={<OrganizationBillingPage />} />
          )}
          {canManageOrganizationBilling && isSelfServePlusPlanEnabled && (
            <Route path={CloudSettingsRoutePaths.Plan} element={<OrganizationPlanPage />} />
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
