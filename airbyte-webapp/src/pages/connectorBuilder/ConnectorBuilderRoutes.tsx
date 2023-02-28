import { Suspense } from "react";
import React from "react";
import { Navigate, Route, Routes } from "react-router-dom";

import { LoadingPage } from "components";

import ConnectorBuilderListPage from "./ConnectorBuilderListPage";

const ConnectorBuilderCreatePage = React.lazy(() => import("./ConnectorBuilderCreatePage"));
const ConnectorBuilderEditPage = React.lazy(() => import("./ConnectorBuilderEditPage"));

export const enum ConnectorBuilderRoutePaths {
  Create = "create",
  Edit = "edit/:projectId",
}

export function getEditPath(projectId: string) {
  return `edit/${projectId}`;
}

const ConnectorBuilderRoutes: React.FC = () => (
  <Suspense fallback={<LoadingPage />}>
    <Routes>
      <Route path={ConnectorBuilderRoutePaths.Edit} element={<ConnectorBuilderEditPage />} />
      <Route path={ConnectorBuilderRoutePaths.Create} element={<ConnectorBuilderCreatePage />} />
      <Route index element={<ConnectorBuilderListPage />} />
      <Route path="*" element={<Navigate to="." replace />} />
    </Routes>
  </Suspense>
);

export default ConnectorBuilderRoutes;
