import React, { PropsWithChildren, Suspense, useMemo } from "react";
import { createSearchParams, Navigate, Route, Routes, useLocation } from "react-router-dom";
import { useEffectOnce } from "react-use";

import LoadingPage from "components/LoadingPage";
import { EnterpriseSourcePage } from "components/source/enterpriseStubs/EnterpriseSourcePage";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useCurrentWorkspace, useInvalidateAllWorkspaceScopeOnChange } from "core/api";
import { usePrefetchCloudWorkspaceData } from "core/api/cloud";
import { DefaultErrorBoundary } from "core/errors";
import { useAnalyticsIdentifyUser, useAnalyticsRegisterValues } from "core/services/analytics/useAnalyticsService";
import { useAuthService } from "core/services/auth";
import { FeatureItem, useFeature } from "core/services/features";
import { isCorporateEmail } from "core/utils/freeEmailProviders";
import { Intent, useGeneratedIntent, useIntent } from "core/utils/rbac";
import { storeUtmFromQuery } from "core/utils/utmStorage";
import { useExperimentContext } from "hooks/services/Experiment";
import { useBuildUpdateCheck } from "hooks/services/useBuildUpdateCheck";
import { useQuery } from "hooks/useQuery";
import ConnectorBuilderRoutes from "pages/connectorBuilder/ConnectorBuilderRoutes";
import { RoutePaths, DestinationPaths, SourcePaths } from "pages/routePaths";
import {
  SourcesPage as SettingsSourcesPage,
  DestinationsPage as SettingsDestinationsPage,
} from "pages/SettingsPage/pages/ConnectorsPage";
import { NotificationPage } from "pages/SettingsPage/pages/NotificationPage";
import { GeneralOrganizationSettingsPage } from "pages/SettingsPage/pages/Organization/GeneralOrganizationSettingsPage";
import { OrganizationMembersPage } from "pages/SettingsPage/pages/Organization/OrganizationMembersPage";

import { AcceptInvitation } from "./AcceptInvitation";
import { CloudRoutes } from "./cloudRoutePaths";
import { LDExperimentServiceProvider } from "./services/thirdParty/launchdarkly";
import { SSOBookmarkPage } from "./views/auth/SSOBookmarkPage";
import { SSOIdentifierPage } from "./views/auth/SSOIdentifierPage";
import { DbtCloudSettingsView } from "./views/settings/integrations/DbtCloudSettingsView";
import { CloudSettingsRoutePaths } from "./views/settings/routePaths";
import { AccountSettingsView } from "./views/users/AccountSettingsView";
import { ApplicationSettingsView } from "./views/users/ApplicationSettingsView/ApplicationSettingsView";
import { DataResidencyView } from "./views/workspaces/DataResidencyView";
import { WorkspaceSettingsView } from "./views/workspaces/WorkspaceSettingsView";

const LoginPage = React.lazy(() => import("./views/auth/LoginPage"));
const SignupPage = React.lazy(() => import("./views/auth/SignupPage"));
const CloudMainView = React.lazy(() => import("packages/cloud/views/layout/CloudMainView"));
const CloudWorkspacesPage = React.lazy(() => import("packages/cloud/views/workspaces"));
const AuthLayout = React.lazy(() => import("packages/cloud/views/auth"));
const OrganizationBillingPage = React.lazy(() => import("packages/cloud/views/billing/OrganizationBillingPage"));
const OrganizationUsagePage = React.lazy(() => import("packages/cloud/views/billing/OrganizationUsagePage"));
const WorkspaceUsagePage = React.lazy(() => import("packages/cloud/views/workspaces/WorkspaceUsagePage"));

const ConnectionsRoutes = React.lazy(() => import("pages/connections/ConnectionsRoutes"));

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
const CloudDefaultView = React.lazy(() => import("./views/CloudDefaultView"));
const CloudSettingsPage = React.lazy(() => import("./views/settings/CloudSettingsPage"));
const AdvancedSettingsPage = React.lazy(() => import("pages/SettingsPage/pages/AdvancedSettingsPage"));

const MainRoutes: React.FC = () => {
  const workspace = useCurrentWorkspace();
  const canViewOrgSettings = useIntent("ViewOrganizationSettings", { organizationId: workspace.organizationId });
  const canManageOrganizationBilling = useGeneratedIntent(Intent.ManageOrganizationBilling);
  const canViewOrganizationUsage = useGeneratedIntent(Intent.ViewOrganizationUsage);

  useExperimentContext("organization", workspace.organizationId);

  const analyticsContext = useMemo(
    () => ({
      workspace_id: workspace.workspaceId,
      customer_id: workspace.customerId,
    }),
    [workspace]
  );
  useAnalyticsRegisterValues(analyticsContext);

  const supportsCloudDbtIntegration = useFeature(FeatureItem.AllowDBTCloudIntegration);
  const supportsDataResidency = useFeature(FeatureItem.AllowChangeDataGeographies);

  return (
    <DefaultErrorBoundary>
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
        <Route path={`${RoutePaths.Settings}/*`} element={<CloudSettingsPage />}>
          <Route path={CloudSettingsRoutePaths.Account} element={<AccountSettingsView />} />
          <Route path={CloudSettingsRoutePaths.Applications} element={<ApplicationSettingsView />} />
          <Route path={CloudSettingsRoutePaths.Workspace} element={<WorkspaceSettingsView />} />
          {supportsDataResidency && (
            <Route path={CloudSettingsRoutePaths.DataResidency} element={<DataResidencyView />} />
          )}
          <Route path={CloudSettingsRoutePaths.Source} element={<SettingsSourcesPage />} />
          <Route path={CloudSettingsRoutePaths.Destination} element={<SettingsDestinationsPage />} />
          <Route path={CloudSettingsRoutePaths.Notifications} element={<NotificationPage />} />
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
        <Route path="*" element={<Navigate to={RoutePaths.Connections} replace />} />
      </Routes>
    </DefaultErrorBoundary>
  );
};

const CloudMainViewRoutes = () => {
  const { loginRedirect } = useQuery<{ loginRedirect: string }>();

  if (loginRedirect) {
    return <Navigate to={loginRedirect} replace />;
  }

  return (
    <Routes>
      <Route path={RoutePaths.Workspaces} element={<CloudWorkspacesPage />} />
      <Route path={CloudRoutes.AcceptInvitation} element={<AcceptInvitation />} />
      <Route
        path={`${RoutePaths.Workspaces}/:workspaceId/*`}
        element={
          <CloudWorkspaceDataPrefetcher>
            <CloudMainView>
              <MainRoutes />
            </CloudMainView>
          </CloudWorkspaceDataPrefetcher>
        }
      />
      <Route path="*" element={<CloudDefaultView />} />
    </Routes>
  );
};

const CloudWorkspaceDataPrefetcher: React.FC<PropsWithChildren<unknown>> = ({ children }) => {
  usePrefetchCloudWorkspaceData();
  return <>{children}</>;
};

export const Routing: React.FC = () => {
  const { user, inited, provider, loggedOut } = useAuthService();
  const workspaceId = useCurrentWorkspaceId();
  const { pathname: originalPathname, search, hash } = useLocation();

  const loginRedirectSearchParam = `${createSearchParams({
    loginRedirect: `${originalPathname}${search}${hash}`,
  })}`;

  const loginRedirectTo =
    loggedOut && originalPathname === "/"
      ? { pathname: CloudRoutes.Login }
      : { pathname: CloudRoutes.Login, search: loginRedirectSearchParam };

  useBuildUpdateCheck();

  // invalidate everything in the workspace scope when the workspaceId changes
  useInvalidateAllWorkspaceScopeOnChange(workspaceId);

  const analyticsContext = useMemo(
    () =>
      user
        ? {
            cloud_user_id: user.userId,
          }
        : null,
    [user]
  );

  const userTraits = useMemo(
    () =>
      user
        ? {
            provider,
            email: user.email,
            isCorporate: isCorporateEmail(user.email),
            currentWorkspaceId: workspaceId,
          }
        : {},
    [provider, user, workspaceId]
  );

  useEffectOnce(() => {
    storeUtmFromQuery(search);
  });

  useAnalyticsRegisterValues(analyticsContext);
  useAnalyticsIdentifyUser(user?.userId, userTraits);

  if (!inited) {
    // Using <LoadingPage /> here causes flickering, because Suspense will immediately render it again
    return null;
  }

  return (
    <LDExperimentServiceProvider>
      <Suspense fallback={<LoadingPage />}>
        <Routes>
          <Route
            path="*"
            element={
              <>
                {/* All routes for non logged in users */}
                {!user && (
                  <AuthLayout>
                    <Suspense fallback={<LoadingPage />}>
                      <Routes>
                        <Route path={CloudRoutes.SsoBookmark} element={<SSOBookmarkPage />} />
                        <Route path={CloudRoutes.Sso} element={<SSOIdentifierPage />} />
                        <Route path={CloudRoutes.Login} element={<LoginPage />} />
                        <Route path={CloudRoutes.Signup} element={<SignupPage />} />
                        {/* In case a not logged in user tries to access anything else navigate them to login */}
                        <Route path="*" element={<Navigate to={loginRedirectTo} />} />
                      </Routes>
                    </Suspense>
                  </AuthLayout>
                )}
                {/* Allow all regular routes if the user is logged in */}
                {user && <CloudMainViewRoutes />}
              </>
            }
          />
        </Routes>
      </Suspense>
    </LDExperimentServiceProvider>
  );
};
