import React, { Suspense } from "react";
import { Navigate, Route, Routes, useLocation, useNavigate } from "react-router-dom";
import { useEffectOnce } from "react-use";

import { LoadingPage } from "components";

import { useCurrentWorkspaceLink } from "area/workspace/utils";

import ConfigureDataActivationConnectionPage from "./ConfigureDataActivationConnectionPage";
import { CreateDataActivationConnectionRoutes } from "./CreateDataActivationConnectionRoutes";
import DataActivationMappingPage from "./DataActivationMappingPage";
import { ConnectionRoutePaths, RoutePaths } from "../routePaths";

const ConfigureConnectionPage = React.lazy(() => import("./ConfigureConnectionPage"));
const ConnectionMappingsPage = React.lazy(() => import("./ConnectionMappingsPage"));
const ConnectionPage = React.lazy(() => import("./ConnectionPage"));
const ConnectionReplicationPage = React.lazy(() => import("./ConnectionReplicationPage"));
const ConnectionSettingsPage = React.lazy(() => import("./ConnectionSettingsPage"));
const ConnectionTimelinePage = React.lazy(() => import("./ConnectionTimelinePage"));
const ConnectionTransformationPage = React.lazy(() => import("./ConnectionTransformationPage"));
const CreateConnectionPage = React.lazy(() => import("./CreateConnectionPage"));

const AllConnectionsPage = React.lazy(() => import("./AllConnectionsPage"));
const StreamStatusPage = React.lazy(() => import("./StreamStatusPage"));

export const JobHistoryToTimelineRedirect = () => {
  const location = useLocation();
  const createLink = useCurrentWorkspaceLink();
  const navigate = useNavigate();

  useEffectOnce(() => {
    const hash = location.hash;
    const pathname = location.pathname;
    const regex = new RegExp(
      `^/${RoutePaths.Workspaces}/[^/]+/${RoutePaths.Connections}/([^/]+)/${ConnectionRoutePaths.JobHistory}`
    );
    const match = pathname.match(regex);
    if (match) {
      const connectionId = match[1];
      const connectionTimelinePath = createLink(
        `/${RoutePaths.Connections}/${connectionId}/${ConnectionRoutePaths.Timeline}`
      );

      if (hash.startsWith("#")) {
        const hashContent = hash.substring(1);
        const [jobId, attemptNumber] = hashContent.includes("::") ? hashContent.split("::") : [hashContent, undefined];
        if (jobId) {
          const searchParams = new URLSearchParams({
            jobId,
            ...(attemptNumber && { attemptNumber }),
            openLogs: "true",
          });
          navigate(`${connectionTimelinePath}?${searchParams.toString()}`, { replace: true });
        }
      } else {
        navigate(connectionTimelinePath, { replace: true });
      }
    }
  });

  return null;
};

export const ConnectionsRoutes: React.FC = () => {
  return (
    <Suspense fallback={<LoadingPage />}>
      <Routes>
        <Route
          index
          path={`${ConnectionRoutePaths.ConnectionNew}/${ConnectionRoutePaths.Configure}/*`}
          element={<ConfigureConnectionPage />}
        />
        <Route
          path={`${ConnectionRoutePaths.ConnectionNew}/${ConnectionRoutePaths.ConfigureDataActivation}`}
          element={<CreateDataActivationConnectionRoutes />}
        >
          <Route index element={<DataActivationMappingPage />} />
          <Route path={ConnectionRoutePaths.ConfigureContinued} element={<ConfigureDataActivationConnectionPage />} />
        </Route>
        <Route path={ConnectionRoutePaths.ConnectionNew} element={<CreateConnectionPage />} />
        <Route path={ConnectionRoutePaths.Root} element={<ConnectionPage />}>
          <Route path={ConnectionRoutePaths.Status} element={<StreamStatusPage />} />
          <Route path={ConnectionRoutePaths.JobHistory} element={<JobHistoryToTimelineRedirect />} />
          <Route path={ConnectionRoutePaths.Timeline} element={<ConnectionTimelinePage />} />
          <Route path={ConnectionRoutePaths.Replication} element={<ConnectionReplicationPage />} />
          <Route path={ConnectionRoutePaths.Mappings} element={<ConnectionMappingsPage />} />
          <Route path={ConnectionRoutePaths.Transformation} element={<ConnectionTransformationPage />} />
          <Route path={ConnectionRoutePaths.Settings} element={<ConnectionSettingsPage />} />
          <Route index element={<Navigate to={ConnectionRoutePaths.Status} replace />} />
        </Route>
        <Route index element={<AllConnectionsPage />} />
      </Routes>
    </Suspense>
  );
};

export default ConnectionsRoutes;
