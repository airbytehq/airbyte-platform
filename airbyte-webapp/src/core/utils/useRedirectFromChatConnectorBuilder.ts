import {
  getLaunchBuilderParamsFromStorage,
  getSetUpNewConnectorParamsFromStorage,
} from "core/utils/connectorChatBuilderStorage";
import { useExperiment } from "hooks/services/Experiment";
import { ConnectorBuilderRoutePaths } from "pages/connectorBuilder/ConnectorBuilderRoutes";
import { RoutePaths, SourcePaths, DestinationPaths } from "pages/routePaths";

const getNewConnectorRedirectUrl = (workspaceId: string) => {
  const setUpNewConnectorParams = getSetUpNewConnectorParamsFromStorage();

  if (!setUpNewConnectorParams) {
    return null;
  }

  if (setUpNewConnectorParams.connectorType === "source") {
    return `/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Source}/${SourcePaths.SelectSourceNew}/${setUpNewConnectorParams.definitionId}`;
  }
  if (setUpNewConnectorParams.connectorType === "destination") {
    return `/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Destination}/${DestinationPaths.SelectDestinationNew}/${setUpNewConnectorParams.definitionId}`;
  }
  return `/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Source}/${SourcePaths.SelectSourceNew}`;
};

const getConnectorBuilderRedirectUrl = (workspaceId: string) => {
  const launchBuilderParams = getLaunchBuilderParamsFromStorage();

  if (!launchBuilderParams) {
    return null;
  }

  return `/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.ConnectorBuilder}/${ConnectorBuilderRoutePaths.Generate}`;
};

export const useRedirectFromChatConnectorBuilder = (workspaceId: string) => {
  const isConnectorBuilderGenerateFromParamsEnabled = useExperiment("connectorBuilder.generateConnectorFromParams");

  if (!isConnectorBuilderGenerateFromParamsEnabled) {
    return null;
  }

  const newConnectorUrl = getNewConnectorRedirectUrl(workspaceId);

  if (newConnectorUrl) {
    return newConnectorUrl;
  }

  return getConnectorBuilderRedirectUrl(workspaceId);
};
