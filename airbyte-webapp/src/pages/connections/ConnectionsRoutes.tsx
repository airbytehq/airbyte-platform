import React, { Suspense } from "react";
import { Navigate, Route, Routes } from "react-router-dom";

import { LoadingPage } from "components";

import { useExperiment } from "hooks/services/Experiment";

import { ConnectionRoutePaths } from "./types";
import { RoutePaths } from "../routePaths";

const CreateConnectionPage = React.lazy(() => import("./CreateConnectionPage"));
const ConnectionPage = React.lazy(() => import("./ConnectionPage"));
const ConnectionReplicationPage = React.lazy(() => import("./ConnectionReplicationPage"));
const ConnectionSettingsPage = React.lazy(() => import("./ConnectionSettingsPage"));
const ConnectionJobHistory = React.lazy(() => import("./ConnectionJobHistoryPage"));
const ConnectionTransformationPage = React.lazy(() => import("./ConnectionTransformationPage"));
const AllConnectionsPage = React.lazy(() => import("./AllConnectionsPage"));

export const ConnectionsRoutes: React.FC = () => {
  const streamCentricUIEnabled = useExperiment("connection.streamCentricUI", false);
  return (
    <Suspense fallback={<LoadingPage />}>
      <Routes>
        <Route path={RoutePaths.ConnectionNew} element={<CreateConnectionPage />} />
        <Route path={ConnectionRoutePaths.Root} element={<ConnectionPage />}>
          <Route
            path={ConnectionRoutePaths.Status}
            element={streamCentricUIEnabled ? null : <ConnectionJobHistory />}
          />
          <Route path={ConnectionRoutePaths.Replication} element={<ConnectionReplicationPage />} />
          <Route path={ConnectionRoutePaths.Transformation} element={<ConnectionTransformationPage />} />
          <Route path={ConnectionRoutePaths.Settings} element={<ConnectionSettingsPage />} />
          <Route path={ConnectionRoutePaths.JobHistory} element={<ConnectionJobHistory />} />
          <Route index element={<Navigate to={ConnectionRoutePaths.Status} replace />} />
        </Route>
        <Route index element={<AllConnectionsPage />} />
      </Routes>
    </Suspense>
  );
};

export default ConnectionsRoutes;
