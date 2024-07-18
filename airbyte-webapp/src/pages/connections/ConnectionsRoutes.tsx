import React, { Suspense } from "react";
import { Navigate, Route, Routes } from "react-router-dom";

import { LoadingPage } from "components";

import { ConnectionRoutePaths } from "../routePaths";

const ConnectionTimelinePage = React.lazy(() => import("./ConnectionTimelinePage"));
const ConfigureConnectionPage = React.lazy(() => import("./ConfigureConnectionPage"));
const CreateConnectionPage = React.lazy(() => import("./CreateConnectionPage"));
const ConnectionPage = React.lazy(() => import("./ConnectionPage"));
const ConnectionReplicationPage = React.lazy(() => import("./ConnectionReplicationPage"));
const ConnectionSettingsPage = React.lazy(() => import("./ConnectionSettingsPage"));
const ConnectionJobHistoryPage = React.lazy(() => import("./ConnectionJobHistoryPage"));
const ConnectionTransformationPage = React.lazy(() => import("./ConnectionTransformationPage"));
const AllConnectionsPage = React.lazy(() => import("./AllConnectionsPage"));
const StreamStatusPage = React.lazy(() => import("./StreamStatusPage"));

export const ConnectionsRoutes: React.FC = () => {
  return (
    <Suspense fallback={<LoadingPage />}>
      <Routes>
        <Route
          index
          path={`${ConnectionRoutePaths.ConnectionNew}/${ConnectionRoutePaths.Configure}/*`}
          element={<ConfigureConnectionPage />}
        />
        <Route path={ConnectionRoutePaths.ConnectionNew} element={<CreateConnectionPage />} />
        <Route path={ConnectionRoutePaths.Root} element={<ConnectionPage />}>
          <Route path={ConnectionRoutePaths.Status} element={<StreamStatusPage />} />
          <Route path={ConnectionRoutePaths.JobHistory} element={<ConnectionJobHistoryPage />} />
          <Route path={ConnectionRoutePaths.Timeline} element={<ConnectionTimelinePage />} />
          <Route path={ConnectionRoutePaths.Replication} element={<ConnectionReplicationPage />} />
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
