import React, { useMemo } from "react";
import { Navigate, Route, Routes, useLocation, useSearchParams } from "react-router-dom";
import { useEffectOnce } from "react-use";

import {
  useGetInstanceConfiguration,
  useInvalidateAllWorkspaceScopeOnChange,
  useListWorkspacesInfinite,
} from "core/api";
import { DefaultErrorBoundary } from "core/errors";
import { useAnalyticsIdentifyUser, useAnalyticsRegisterValues } from "core/services/analytics";
import { useAuthService } from "core/services/auth";
import { FeatureItem, useFeature } from "core/services/features";
import { useIntent } from "core/utils/rbac/intent";
import { storeUtmFromQuery } from "core/utils/utmStorage";
import { useApiHealthPoll } from "hooks/services/Health";
import { useBuildUpdateCheck } from "hooks/services/useBuildUpdateCheck";
import { useCurrentWorkspace } from "hooks/services/useWorkspace";
import { ApplicationSettingsView } from "packages/cloud/views/users/ApplicationSettingsView/ApplicationSettingsView";
import MainView from "views/layout/MainView";

import { RoutePaths, DestinationPaths, SourcePaths, SettingsRoutePaths } from "./routePaths";
import { GeneralOrganizationSettingsPage } from "./SettingsPage/GeneralOrganizationSettingsPage";
import { GeneralWorkspaceSettingsPage } from "./SettingsPage/GeneralWorkspaceSettingsPage";
import { AccountPage } from "./SettingsPage/pages/AccountPage";
import { DestinationsPage, SourcesPage } from "./SettingsPage/pages/ConnectorsPage";
import { MetricsPage } from "./SettingsPage/pages/MetricsPage";
import { NotificationPage } from "./SettingsPage/pages/NotificationPage";
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
  useAnalyticsIdentifyUser(workspace.workspaceId, {
    protocol: window.location.protocol,
    isLocalhost: window.location.hostname === "localhost" || window.location.hostname === "127.0.0.1",
  });
};

const MainViewRoutes: React.FC = () => {
  const { organizationId, workspaceId } = useCurrentWorkspace();
  const multiWorkspaceUI = useFeature(FeatureItem.MultiWorkspaceUI);
  const isTokenManagementEnabled = useFeature(FeatureItem.APITokenManagement);
  const canViewWorkspaceSettings = useIntent("ViewWorkspaceSettings", { workspaceId });
  const canViewOrganizationSettings = useIntent("ViewOrganizationSettings", { organizationId });

  return (
    <MainView>
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
            <Route path={SourcePaths.Root} element={<SourceItemPage />}>
              <Route index element={<SourceSettingsPage />} />
              <Route path={SourcePaths.Connections} element={<SourceConnectionsPage />} />
            </Route>
          </Route>
          <Route path={`${RoutePaths.Connections}/*`} element={<ConnectionsRoutes />} />
          <Route path={`${RoutePaths.Settings}/*`} element={<SettingsPage />}>
            <Route path={SettingsRoutePaths.Account} element={<AccountPage />} />
            {isTokenManagementEnabled && (
              <Route path={SettingsRoutePaths.Applications} element={<ApplicationSettingsView />} />
            )}
            {canViewWorkspaceSettings && multiWorkspaceUI && (
              <Route path={SettingsRoutePaths.Workspace} element={<GeneralWorkspaceSettingsPage />} />
            )}
            {canViewWorkspaceSettings && (
              <>
                <Route path={SettingsRoutePaths.Source} element={<SourcesPage />} />
                <Route path={SettingsRoutePaths.Destination} element={<DestinationsPage />} />
              </>
            )}
            <Route path={SettingsRoutePaths.Notifications} element={<NotificationPage />} />
            <Route path={SettingsRoutePaths.Metrics} element={<MetricsPage />} />
            {multiWorkspaceUI && canViewOrganizationSettings && (
              <Route path={SettingsRoutePaths.Organization} element={<GeneralOrganizationSettingsPage />} />
            )}
            <Route path={SettingsRoutePaths.Advanced} element={<AdvancedSettingsPage />} />
            <Route path="*" element={<Navigate to={SettingsRoutePaths.Account} replace />} />
          </Route>
          <Route path={`${RoutePaths.ConnectorBuilder}/*`} element={<ConnectorBuilderRoutes />} />

          <Route path="*" element={<Navigate to={RoutePaths.Connections} />} />
        </Routes>
      </DefaultErrorBoundary>
    </MainView>
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

const RoutingWithWorkspace: React.FC<{ element?: JSX.Element }> = ({ element }) => {
  const workspace = useCurrentWorkspace();

  useAddAnalyticsContextForWorkspace(workspace);
  useApiHealthPoll();

  // invalidate everything in the workspace scope when the workspaceId changes
  useInvalidateAllWorkspaceScopeOnChange(workspace.workspaceId);

  return element ?? <MainViewRoutes />;
};

export const Routing: React.FC = () => {
  const { inited, user } = useAuthService();

  useBuildUpdateCheck();
  const { search } = useLocation();

  useEffectOnce(() => {
    storeUtmFromQuery(search);
  });

  const multiWorkspaceUI = useFeature(FeatureItem.MultiWorkspaceUI);
  const { initialSetupComplete } = useGetInstanceConfiguration();

  if (!inited) {
    return null;
  }

  return (
    <Routes>
      {user && !initialSetupComplete ? (
        <Route path="*" element={<PreferencesRoutes />} />
      ) : (
        <>
          {multiWorkspaceUI && <Route path={RoutePaths.Workspaces} element={<WorkspacesPage />} />}
          <Route path="/" element={<DefaultView />} />
          <Route path={`${RoutePaths.Workspaces}/:workspaceId/*`} element={<RoutingWithWorkspace />} />
          <Route path="*" element={<AutoSelectFirstWorkspace />} />
        </>
      )}
    </Routes>
  );
};
