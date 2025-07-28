import React from "react";
import { Navigate, Route, Routes } from "react-router-dom";

import { EnterpriseSourcePage } from "components/source/enterpriseStubs/EnterpriseSourcePage";

import { UserSettingsRoutes } from "area/settings/UserSettingsRoutes";
import { useCurrentWorkspace } from "core/api";
import { usePrefetchWorkspaceData } from "core/api/cloud";
import { useAnalyticsRegisterValues } from "core/services/analytics/useAnalyticsService";
import { FeatureItem, useFeature } from "core/services/features";
import { Intent, useGeneratedIntent, useIntent } from "core/utils/rbac";
import { useExperiment, useExperimentContext } from "hooks/services/Experiment";
import OrganizationBillingPage from "packages/cloud/views/billing/OrganizationBillingPage";
import OrganizationUsagePage from "packages/cloud/views/billing/OrganizationUsagePage";
import { CloudSettingsPage } from "packages/cloud/views/settings/CloudSettingsPage";
import { DbtCloudSettingsView } from "packages/cloud/views/settings/integrations/DbtCloudSettingsView";
import { CloudSettingsRoutePaths } from "packages/cloud/views/settings/routePaths";
import { AccountSettingsView } from "packages/cloud/views/users/AccountSettingsView";
import { ApplicationSettingsView } from "packages/cloud/views/users/ApplicationSettingsView/ApplicationSettingsView";
import { WorkspaceSettingsView } from "packages/cloud/views/workspaces/WorkspaceSettingsView";
import WorkspaceUsagePage from "packages/cloud/views/workspaces/WorkspaceUsagePage";
import { OnboardingPage } from "pages/OnboardingPage/OnboardingPage";
import { RoutePaths, DestinationPaths, SourcePaths, SettingsRoutePaths } from "pages/routePaths";
import AdvancedSettingsPage from "pages/SettingsPage/pages/AdvancedSettingsPage";
import {
  SourcesPage as SettingsSourcesPage,
  DestinationsPage as SettingsDestinationsPage,
} from "pages/SettingsPage/pages/ConnectorsPage";
import { EmbeddedSettingsPage } from "pages/SettingsPage/pages/EmbbededSettingsPage/EmbeddedSettingsPage";
import { NotificationPage } from "pages/SettingsPage/pages/NotificationPage";
import { GeneralOrganizationSettingsPage } from "pages/SettingsPage/pages/Organization/GeneralOrganizationSettingsPage";
import { OrganizationMembersPage } from "pages/SettingsPage/pages/Organization/OrganizationMembersPage";
import { WorkspaceMembersPage } from "pages/SettingsPage/Workspace/WorkspaceMembersPage";

// Lazy loaded components
const AllDestinationsPage = React.lazy(() => import("pages/destination/AllDestinationsPage"));
const CreateDestinationPage = React.lazy(() => import("pages/destination/CreateDestinationPage"));
const SelectDestinationPage = React.lazy(() => import("pages/destination/SelectDestinationPage"));
const DestinationItemPage = React.lazy(() => import("pages/destination/DestinationItemPage"));
const DestinationConnectionsPage = React.lazy(() => import("pages/destination/DestinationConnectionsPage"));
const DestinationSettingsPage = React.lazy(() => import("pages/destination/DestinationSettingsPage"));

const AllSourcesPage = React.lazy(() => import("pages/source/AllSourcesPage"));
const CreateSourcePage = React.lazy(() => import("pages/source/CreateSourcePage"));
const SelectSourcePage = React.lazy(() => import("pages/source/SelectSourcePage"));
const SourceItemPage = React.lazy(() => import("pages/source/SourceItemPage"));
const SourceConnectionsPage = React.lazy(() => import("pages/source/SourceConnectionsPage"));
const SourceSettingsPage = React.lazy(() => import("pages/source/SourceSettingsPage"));

const ConnectionsRoutes = React.lazy(() => import("pages/connections/ConnectionsRoutes"));
const ConnectorBuilderRoutes = React.lazy(() => import("pages/connectorBuilder/ConnectorBuilderRoutes"));

export const WorkspacesRoutes: React.FC = () => {
  usePrefetchWorkspaceData();
  const workspace = useCurrentWorkspace();
  const canViewOrgSettings = useIntent("ViewOrganizationSettings", { organizationId: workspace.organizationId });
  const canManageOrganizationBilling = useGeneratedIntent(Intent.ManageOrganizationBilling);
  const canViewOrganizationUsage = useGeneratedIntent(Intent.ViewOrganizationUsage);
  const showOnboarding = useExperiment("onboarding.surveyEnabled");

  useExperimentContext("organization", workspace.organizationId);

  const analyticsContext = React.useMemo(
    () => ({
      workspace_id: workspace.workspaceId,
      customer_id: workspace.customerId,
    }),
    [workspace]
  );
  useAnalyticsRegisterValues(analyticsContext);

  const supportsCloudDbtIntegration = useFeature(FeatureItem.AllowDBTCloudIntegration);

  return (
    <Routes>
      <Route path={RoutePaths.Destination}>
        <Route index element={<AllDestinationsPage />} />
        <Route path={DestinationPaths.SelectDestinationNew} element={<SelectDestinationPage />} />
        <Route path={DestinationPaths.DestinationNew} element={<CreateDestinationPage />} />
        <Route path={DestinationPaths.Root} element={<DestinationItemPage />}>
          <Route index element={<DestinationSettingsPage />} />
          <Route path={DestinationPaths.Connections} element={<DestinationConnectionsPage />} />
        </Route>
      </Route>
      <Route path={RoutePaths.Source}>
        <Route index element={<AllSourcesPage />} />
        <Route path={SourcePaths.SelectSourceNew} element={<SelectSourcePage />} />
        <Route path={SourcePaths.SourceNew} element={<CreateSourcePage />} />
        <Route path={SourcePaths.EnterpriseSource} element={<EnterpriseSourcePage />} />
        <Route path={SourcePaths.Root} element={<SourceItemPage />}>
          <Route index element={<SourceSettingsPage />} />
          <Route path={SourcePaths.Connections} element={<SourceConnectionsPage />} />
        </Route>
      </Route>
      <Route path={`${RoutePaths.Connections}/*`} element={<ConnectionsRoutes />} />
      {showOnboarding && <Route path={RoutePaths.Onboarding} element={<OnboardingPage />} />}
      <Route path={`${RoutePaths.Settings}/*`} element={<CloudSettingsPage />}>
        <Route path={CloudSettingsRoutePaths.Account} element={<AccountSettingsView />} />
        <Route path={CloudSettingsRoutePaths.Applications} element={<ApplicationSettingsView />} />
        <Route path={CloudSettingsRoutePaths.Workspace} element={<WorkspaceSettingsView />} />
        <Route path={CloudSettingsRoutePaths.WorkspaceMembers} element={<WorkspaceMembersPage />} />
        <Route path={CloudSettingsRoutePaths.Source} element={<SettingsSourcesPage />} />
        <Route path={CloudSettingsRoutePaths.Destination} element={<SettingsDestinationsPage />} />
        <Route path={CloudSettingsRoutePaths.Notifications} element={<NotificationPage />} />
        <Route path={SettingsRoutePaths.Embedded} element={<EmbeddedSettingsPage />} />
        {supportsCloudDbtIntegration && (
          <Route path={CloudSettingsRoutePaths.DbtCloud} element={<DbtCloudSettingsView />} />
        )}
        <Route path={CloudSettingsRoutePaths.Usage} element={<WorkspaceUsagePage />} />
        {canViewOrgSettings && (
          <>
            <Route path={CloudSettingsRoutePaths.Organization} element={<GeneralOrganizationSettingsPage />} />
            <Route path={CloudSettingsRoutePaths.OrganizationMembers} element={<OrganizationMembersPage />} />
          </>
        )}
        {canManageOrganizationBilling && (
          <Route path={CloudSettingsRoutePaths.Billing} element={<OrganizationBillingPage />} />
        )}
        {canViewOrganizationUsage && (
          <Route path={CloudSettingsRoutePaths.OrganizationUsage} element={<OrganizationUsagePage />} />
        )}
        <Route path={CloudSettingsRoutePaths.Advanced} element={<AdvancedSettingsPage />} />
        <Route path="*" element={<Navigate to={CloudSettingsRoutePaths.Account} replace />} />
      </Route>
      <Route path={`${RoutePaths.ConnectorBuilder}/*`} element={<ConnectorBuilderRoutes />} />
      <Route path={`${SettingsRoutePaths.User}/*`} element={<UserSettingsRoutes />} />
      <Route path="*" element={<Navigate to={RoutePaths.Connections} replace />} />
    </Routes>
  );
};
