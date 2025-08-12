import React, { useMemo } from "react";
import { createSearchParams, Navigate, Route, Routes, useLocation, useSearchParams } from "react-router-dom";
import { useEffectOnce } from "react-use";

import { EnterpriseSourcePage } from "components/source/enterpriseStubs/EnterpriseSourcePage";

import MainLayout from "area/layout/MainLayout";
import { useCurrentWorkspaceId } from "area/workspace/utils";
import {
  useGetInstanceConfiguration,
  useInvalidateAllWorkspaceScopeOnChange,
  useListWorkspacesInfinite,
} from "core/api";
import { DefaultErrorBoundary } from "core/errors";
import { useAnalyticsIdentifyUser, useAnalyticsRegisterValues } from "core/services/analytics";
import { useAuthService } from "core/services/auth";
import { FeatureItem, useFeature } from "core/services/features";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
import { useIntent } from "core/utils/rbac/intent";
import { useEnterpriseLicenseCheck } from "core/utils/useEnterpriseLicenseCheck";
import { storeUtmFromQuery } from "core/utils/utmStorage";
import { useExperiment } from "hooks/services/Experiment";
import { useApiHealthPoll } from "hooks/services/Health";
import { useBuildUpdateCheck } from "hooks/services/useBuildUpdateCheck";
import { useCurrentWorkspace } from "hooks/services/useWorkspace";
import { useQuery } from "hooks/useQuery";
import { ApplicationSettingsView } from "packages/cloud/views/users/ApplicationSettingsView/ApplicationSettingsView";
import { LoginPage } from "pages/login/LoginPage";
import MainView from "views/layout/MainView";

import { EmbeddedSourceCreatePage } from "./embedded/EmbeddedSourceCreatePage/EmbeddedSourcePage";
import { OnboardingPage } from "./OnboardingPage/OnboardingPage";
import { OrganizationRoutes } from "./organization/OrganizationRoutes";
import { RoutePaths, DestinationPaths, SourcePaths, SettingsRoutePaths } from "./routePaths";
import { AccountPage } from "./SettingsPage/pages/AccountPage";
import { DestinationsPage, SourcesPage } from "./SettingsPage/pages/ConnectorsPage";
import { LicenseSettingsPage } from "./SettingsPage/pages/LicenseDetailsPage/LicenseSettingsPage";
import { MetricsPage } from "./SettingsPage/pages/MetricsPage";
import { NotificationPage } from "./SettingsPage/pages/NotificationPage";
import { GeneralOrganizationSettingsPage } from "./SettingsPage/pages/Organization/GeneralOrganizationSettingsPage";
import { OrganizationMembersPage } from "./SettingsPage/pages/Organization/OrganizationMembersPage";
import { GeneralWorkspaceSettingsPage } from "./SettingsPage/Workspace/GeneralWorkspaceSettingsPage";
import { WorkspaceMembersPage } from "./SettingsPage/Workspace/WorkspaceMembersPage";
import { WorkspaceRead } from "../core/api/types/AirbyteClient";

const DefaultView = React.lazy(() => import("./DefaultView"));
const ConnectionsRoutes = React.lazy(() => import("./connections/ConnectionsRoutes"));
const ConnectorBuilderRoutes = React.lazy(() => import("./connectorBuilder/ConnectorBuilderRoutes"));
const AllDestinationsPage = React.lazy(() => import("./destination/AllDestinationsPage"));
const CreateDestinationPage = React.lazy(() => import("./destination/CreateDestinationPage"));
const SelectDestinationPage = React.lazy(() => import("./destination/SelectDestinationPage"));
const DestinationItemPage = React.lazy(() => import("./destination/DestinationItemPage"));
const DestinationConnectionsPage = React.lazy(() => import("./destination/DestinationConnectionsPage"));
const DestinationSettingsPage = React.lazy(() => import("./destination/DestinationSettingsPage"));
const SetupPage = React.lazy(() => import("./SetupPage"));
const SettingsPage = React.lazy(() => import("./SettingsPage"));
const AllSourcesPage = React.lazy(() => import("./source/AllSourcesPage"));
const CreateSourcePage = React.lazy(() => import("./source/CreateSourcePage"));
const SelectSourcePage = React.lazy(() => import("./source/SelectSourcePage"));
const SourceItemPage = React.lazy(() => import("./source/SourceItemPage"));
const SourceSettingsPage = React.lazy(() => import("./source/SourceSettingsPage"));
const SourceConnectionsPage = React.lazy(() => import("./source/SourceConnectionsPage"));
const AdvancedSettingsPage = React.lazy(() => import("./SettingsPage/pages/AdvancedSettingsPage"));

const WorkspacesPage = React.lazy(() => import("./workspaces"));

const useAddAnalyticsContextForWorkspace = (workspace: WorkspaceRead): void => {
  const analyticsContext = useMemo(
    () => ({
      workspace_id: workspace.workspaceId,
      customer_id: workspace.customerId,
    }),
    [workspace.workspaceId, workspace.customerId]
  );
  useAnalyticsRegisterValues(analyticsContext);
  const userTraits = useMemo(
    () => ({
      protocol: window.location.protocol,
      isLocalhost: window.location.hostname === "localhost" || window.location.hostname === "127.0.0.1",
    }),
    []
  );
  useAnalyticsIdentifyUser(workspace.workspaceId, userTraits);
};

const WorkspacesRoutes: React.FC = () => {
  const workspace = useCurrentWorkspace();
  useAddAnalyticsContextForWorkspace(workspace);

  const { organizationId } = workspace;
  const multiWorkspaceUI = useFeature(FeatureItem.MultiWorkspaceUI);
  const { applicationSupport } = useAuthService();
  const licenseSettings = useFeature(FeatureItem.EnterpriseLicenseChecking);
  const isAccessManagementEnabled = useFeature(FeatureItem.RBAC);
  const displayOrganizationUsers = useFeature(FeatureItem.DisplayOrganizationUsers);
  const canViewWorkspaceSettings = useGeneratedIntent(Intent.ViewWorkspaceSettings);
  const canViewOrganizationSettings = useIntent("ViewOrganizationSettings", { organizationId });

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
        <Route path="onboarding" element={<OnboardingPage />} />
        <Route path={`${RoutePaths.Settings}/*`} element={<SettingsPage />}>
          <Route path={SettingsRoutePaths.Account} element={<AccountPage />} />
          {applicationSupport !== "none" && (
            <Route path={SettingsRoutePaths.Applications} element={<ApplicationSettingsView />} />
          )}
          <Route path={SettingsRoutePaths.Workspace} element={<GeneralWorkspaceSettingsPage />} />
          {canViewWorkspaceSettings && multiWorkspaceUI ? (
            <Route path={SettingsRoutePaths.WorkspaceMembers} element={<WorkspaceMembersPage />} />
          ) : null}
          {canViewWorkspaceSettings && (
            <>
              <Route path={SettingsRoutePaths.Source} element={<SourcesPage />} />
              <Route path={SettingsRoutePaths.Destination} element={<DestinationsPage />} />
            </>
          )}
          <Route path={SettingsRoutePaths.Notifications} element={<NotificationPage />} />
          <Route path={SettingsRoutePaths.Metrics} element={<MetricsPage />} />
          {multiWorkspaceUI && canViewOrganizationSettings && (
            <>
              <Route path={SettingsRoutePaths.Organization} element={<GeneralOrganizationSettingsPage />} />
              {isAccessManagementEnabled && displayOrganizationUsers && (
                <Route path={SettingsRoutePaths.OrganizationMembers} element={<OrganizationMembersPage />} />
              )}
            </>
          )}
          {licenseSettings && <Route path={SettingsRoutePaths.License} element={<LicenseSettingsPage />} />}
          <Route path={SettingsRoutePaths.Advanced} element={<AdvancedSettingsPage />} />
          <Route path="*" element={<Navigate to={SettingsRoutePaths.Account} replace />} />
        </Route>
        <Route path={`${RoutePaths.ConnectorBuilder}/*`} element={<ConnectorBuilderRoutes />} />
        <Route path="*" element={<Navigate to={RoutePaths.Connections} />} />
      </Routes>
    </DefaultErrorBoundary>
  );
};

const PreferencesRoutes = () => (
  <Routes>
    <Route path={RoutePaths.Setup} element={<SetupPage />} />
    <Route path="*" element={<Navigate to={RoutePaths.Setup} />} />
  </Routes>
);

export const AutoSelectFirstWorkspace: React.FC = () => {
  const location = useLocation();
  const { data: workspacesData } = useListWorkspacesInfinite(2, "", true);
  const workspaces = workspacesData?.pages?.[0]?.data.workspaces ?? [];

  const currentWorkspace = workspaces.length ? workspaces[0] : undefined;

  const [searchParams] = useSearchParams();

  return (
    <Navigate
      to={{
        pathname: currentWorkspace
          ? `/${RoutePaths.Workspaces}/${currentWorkspace.workspaceId}${location.pathname}`
          : `/${RoutePaths.Workspaces}`,
        search: `?${searchParams.toString()}`,
      }}
      replace
    />
  );
};

export const Routing: React.FC = () => {
  const { pathname: originalPathname, search, hash } = useLocation();
  const { inited, loggedOut } = useAuthService();
  const { initialSetupComplete } = useGetInstanceConfiguration();
  useBuildUpdateCheck();
  useApiHealthPoll();

  // invalidate everything in the workspace scope when the workspaceId changes
  const workspaceId = useCurrentWorkspaceId();
  useInvalidateAllWorkspaceScopeOnChange(workspaceId);

  useEffectOnce(() => {
    storeUtmFromQuery(search);
  });

  if (!inited) {
    return null;
  }

  if (loggedOut) {
    const loginRedirectSearchParam = `${createSearchParams({
      loginRedirect: `${originalPathname}${search}${hash}`,
    })}`;
    const loginRedirectTo =
      loggedOut && originalPathname === "/"
        ? { pathname: RoutePaths.Login }
        : { pathname: RoutePaths.Login, search: loginRedirectSearchParam };

    return (
      <Routes>
        {!initialSetupComplete ? (
          <Route path="*" element={<PreferencesRoutes />} />
        ) : (
          <>
            <Route path={RoutePaths.Login} element={<LoginPage />} />
            <Route path="*" element={<Navigate to={loginRedirectTo} />} />
          </>
        )}
      </Routes>
    );
  }

  return <AuthenticatedRoutes />;
};

const AuthenticatedRoutes = () => {
  const { loginRedirect } = useQuery<{ loginRedirect: string }>();
  const multiWorkspaceUI = useFeature(FeatureItem.MultiWorkspaceUI);
  const { initialSetupComplete } = useGetInstanceConfiguration();
  useEnterpriseLicenseCheck();
  const isOrgPickerEnabled = useExperiment("sidebar.showOrgPicker");

  if (loginRedirect) {
    return <Navigate to={loginRedirect} replace />;
  }

  return (
    <Routes>
      <Route path={`/${RoutePaths.EmbeddedWidget}`} element={<EmbeddedSourceCreatePage />} />
      {!initialSetupComplete ? (
        <Route path="*" element={<PreferencesRoutes />} />
      ) : isOrgPickerEnabled ? (
        <Route element={<MainLayout />}>
          <Route path="account" element={<div>User Routes</div>} />
          <Route path={`${RoutePaths.Organization}/:organizationId/*`} element={<OrganizationRoutes />} />
          <Route path={`${RoutePaths.Workspaces}/:workspaceId/*`} element={<WorkspacesRoutes />} />
          <Route path="*" element={<DefaultView />} />
        </Route>
      ) : (
        <>
          {multiWorkspaceUI && <Route path={RoutePaths.Workspaces} element={<WorkspacesPage />} />}
          <Route path="/" element={<DefaultView />} />
          <Route element={<MainView />}>
            <Route path={`${RoutePaths.Workspaces}/:workspaceId/*`} element={<WorkspacesRoutes />} />
          </Route>
          <Route path="*" element={<AutoSelectFirstWorkspace />} />
        </>
      )}
    </Routes>
  );
};
