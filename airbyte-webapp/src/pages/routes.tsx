import React, { useMemo } from "react";
import { Navigate, Route, Routes, useLocation, useSearchParams } from "react-router-dom";
import { useEffectOnce } from "react-use";

import { ApiErrorBoundary } from "components/common/ApiErrorBoundary";

import {
  useGetInstanceConfiguration,
  useInvalidateAllWorkspaceScopeOnChange,
  useListWorkspacesInfinite,
} from "core/api";
import { useAnalyticsIdentifyUser, useAnalyticsRegisterValues } from "core/services/analytics";
import { useAuthService } from "core/services/auth";
import { FeatureItem, useFeature } from "core/services/features";
import { storeUtmFromQuery } from "core/utils/utmStorage";
import { useApiHealthPoll } from "hooks/services/Health";
import { useBuildUpdateCheck } from "hooks/services/useBuildUpdateCheck";
import { useCurrentWorkspace } from "hooks/services/useWorkspace";
import { CompleteOauthRequest } from "views/CompleteOauthRequest";
import MainView from "views/layout/MainView";

import { RoutePaths, DestinationPaths, SourcePaths } from "./routePaths";
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

const WorkspacesPage = React.lazy(() => import("./workspaces/WorkspacesPage"));

const useAddAnalyticsContextForWorkspace = (workspace: WorkspaceRead): void => {
  const analyticsContext = useMemo(
    () => ({
      workspace_id: workspace.workspaceId,
      customer_id: workspace.customerId,
    }),
    [workspace.workspaceId, workspace.customerId]
  );
  useAnalyticsRegisterValues(analyticsContext);
  useAnalyticsIdentifyUser(workspace.workspaceId);
};

const MainViewRoutes: React.FC = () => {
  return (
    <MainView>
      <ApiErrorBoundary>
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
          <Route path={`${RoutePaths.Settings}/*`} element={<SettingsPage />} />
          <Route path={`${RoutePaths.ConnectorBuilder}/*`} element={<ConnectorBuilderRoutes />} />

          <Route path="*" element={<Navigate to={RoutePaths.Connections} />} />
        </Routes>
      </ApiErrorBoundary>
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
      <Route path={RoutePaths.AuthFlow} element={<CompleteOauthRequest />} />
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
