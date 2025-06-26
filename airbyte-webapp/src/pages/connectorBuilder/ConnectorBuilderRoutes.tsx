import React, { Suspense } from "react";
import { Navigate, Route, Routes } from "react-router-dom";

import { LoadingPage } from "components";

import { useLocalStorage } from "core/utils/useLocalStorage";
import { useExperiment } from "hooks/services/Experiment";

import ConnectorBuilderListPage from "./ConnectorBuilderListPage";

const ConnectorBuilderCreatePage = React.lazy(() => import("./ConnectorBuilderCreatePage"));
const ConnectorBuilderGeneratePage = React.lazy(() => import("./ConnectorBuilderGeneratePage"));
const ConnectorBuilderForkPage = React.lazy(() => import("./ConnectorBuilderForkPage"));
const ConnectorBuilderEditPage = React.lazy(() => import("./ConnectorBuilderEditPage"));

const ConnectorBuilderDeprecatedRoutes = React.lazy(
  () => import("../connectorBuilder__deprecated/ConnectorBuilderRoutes")
);

export const enum ConnectorBuilderRoutePaths {
  Create = "create",
  Generate = "generate",
  Fork = "fork",
  Edit = "edit/:projectId",
}

export function getEditPath(projectId: string) {
  return `edit/${projectId}`;
}

const ConnectorBuilderRoutes: React.FC = () => {
  const [advancedMode] = useLocalStorage("airbyte_connector-builder-advanced-mode", false);
  const isSchemaFormEnabled = useExperiment("connectorBuilder.schemaForm");

  if (!advancedMode || !isSchemaFormEnabled) {
    return <ConnectorBuilderDeprecatedRoutes />;
  }

  return (
    <Suspense fallback={<LoadingPage />}>
      <Routes>
        <Route path={ConnectorBuilderRoutePaths.Edit} element={<ConnectorBuilderEditPage />} />
        <Route path={ConnectorBuilderRoutePaths.Create} element={<ConnectorBuilderCreatePage />} />
        <Route path={ConnectorBuilderRoutePaths.Generate} element={<ConnectorBuilderGeneratePage />} />
        <Route path={ConnectorBuilderRoutePaths.Fork} element={<ConnectorBuilderForkPage />} />
        <Route index element={<ConnectorBuilderListPage />} />
        <Route path="*" element={<Navigate to="." replace />} />
      </Routes>
    </Suspense>
  );
};

export default ConnectorBuilderRoutes;
