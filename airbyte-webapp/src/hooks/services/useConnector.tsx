import { useMutation } from "@tanstack/react-query";
import { useMemo } from "react";

import { useConfig } from "config";
import { ConnectionConfiguration } from "core/domain/connection";
import { DestinationService } from "core/domain/connector/DestinationService";
import { SourceService } from "core/domain/connector/SourceService";
import { useGetOutOfDateConnectorsCount } from "services/connector/ConnectorDefinitions";
import {
  useDestinationDefinitionList,
  useLatestDestinationDefinitionList,
  useUpdateDestinationDefinition,
} from "services/connector/DestinationDefinitionService";
import {
  useLatestSourceDefinitionList,
  useSourceDefinitionList,
  useUpdateSourceDefinition,
} from "services/connector/SourceDefinitionService";
import { useDefaultRequestMiddlewares } from "services/useDefaultRequestMiddlewares";
import { useInitService } from "services/useInitService";
import { useCurrentWorkspaceId } from "services/workspaces/WorkspacesService";

import { CheckConnectionRead } from "../../core/request/AirbyteClient";

export const useUpdateAllConnectors = (connectorType: "sources" | "destinations") => {
  const workspaceId = useCurrentWorkspaceId();
  const { updateAllSourceVersions } = useUpdateSourceDefinitions();
  const { updateAllDestinationVersions } = useUpdateDestinationDefinitions();
  return useMutation(["updateAllConnectors", workspaceId], async () =>
    connectorType === "sources" ? updateAllSourceVersions() : updateAllDestinationVersions()
  );
};

export const useUpdateSourceDefinitions = () => {
  const { sourceDefinitions } = useSourceDefinitionList();
  const { sourceDefinitions: latestSourceDefinitions } = useLatestSourceDefinitionList();
  const { mutateAsync: updateSourceDefinition } = useUpdateSourceDefinition();

  const newSourceDefinitions = useMemo(
    () =>
      latestSourceDefinitions.filter(
        (latestDefinition) =>
          sourceDefinitions.find((definition) => definition.sourceDefinitionId === latestDefinition.sourceDefinitionId)
            ?.dockerImageTag !== latestDefinition.dockerImageTag
      ),
    [sourceDefinitions, latestSourceDefinitions]
  );

  const updateAllSourceVersions = async () => {
    await Promise.all(
      newSourceDefinitions?.map((item) =>
        updateSourceDefinition({
          sourceDefinitionId: item.sourceDefinitionId,
          dockerImageTag: item.dockerImageTag,
        })
      )
    );
  };

  return { updateAllSourceVersions };
};

export const useUpdateDestinationDefinitions = () => {
  const { destinationDefinitions } = useDestinationDefinitionList();
  const { destinationDefinitions: latestDestinationDefinitions } = useLatestDestinationDefinitionList();
  const { mutateAsync: updateDestinationDefinition } = useUpdateDestinationDefinition();

  const newDestinationDefinitions = useMemo(
    () =>
      latestDestinationDefinitions.filter(
        (latestDefinition) =>
          destinationDefinitions.find(
            (definition) => definition.destinationDefinitionId === latestDefinition.destinationDefinitionId
          )?.dockerImageTag !== latestDefinition.dockerImageTag
      ),
    [destinationDefinitions, latestDestinationDefinitions]
  );

  const updateAllDestinationVersions = async () => {
    await Promise.all(
      newDestinationDefinitions?.map((item) =>
        updateDestinationDefinition({
          destinationDefinitionId: item.destinationDefinitionId,
          dockerImageTag: item.dockerImageTag,
        })
      )
    );
  };

  return { updateAllDestinationVersions };
};

export const useGetConnectorsOutOfDate = () => {
  const { data: outOfDateConnectors } = useGetOutOfDateConnectorsCount();

  if (!outOfDateConnectors) {
    return {
      hasNewVersions: false,
      hasNewSourceVersion: false,
      hasNewDestinationVersion: false,
      countNewSourceVersion: 0,
      countNewDestinationVersion: 0,
      outOfDateConnectors: undefined,
    };
  }

  const hasNewSourceVersion = outOfDateConnectors?.sourceDefinitions > 0;
  const hasNewDestinationVersion = outOfDateConnectors?.destinationDefinitions > 0;
  const hasNewVersions = hasNewSourceVersion || hasNewDestinationVersion;

  return {
    hasNewVersions,
    hasNewSourceVersion,
    hasNewDestinationVersion,
    countNewSourceVersion: outOfDateConnectors.sourceDefinitions,
    countNewDestinationVersion: outOfDateConnectors.destinationDefinitions,
    outOfDateConnectors,
  };
};

function useGetDestinationService(): DestinationService {
  const { apiUrl } = useConfig();
  const requestAuthMiddleware = useDefaultRequestMiddlewares();

  return useInitService(() => new DestinationService(apiUrl, requestAuthMiddleware), [apiUrl, requestAuthMiddleware]);
}

function useGetSourceService(): SourceService {
  const { apiUrl } = useConfig();
  const requestAuthMiddleware = useDefaultRequestMiddlewares();

  return useInitService(() => new SourceService(apiUrl, requestAuthMiddleware), [apiUrl, requestAuthMiddleware]);
}

export type CheckConnectorParams = { signal: AbortSignal } & (
  | { selectedConnectorId: string }
  | {
      selectedConnectorId: string;
      name: string;
      connectionConfiguration: ConnectionConfiguration;
    }
  | {
      selectedConnectorDefinitionId: string;
      connectionConfiguration: ConnectionConfiguration;
      workspaceId: string;
    }
);

export const useCheckConnector = (formType: "source" | "destination") => {
  const destinationService = useGetDestinationService();
  const sourceService = useGetSourceService();

  return useMutation<CheckConnectionRead, Error, CheckConnectorParams>(async (params: CheckConnectorParams) => {
    const payload: Record<string, unknown> = {};

    if ("connectionConfiguration" in params) {
      payload.connectionConfiguration = params.connectionConfiguration;
    }

    if ("name" in params) {
      payload.name = params.name;
    }

    if ("workspaceId" in params) {
      payload.workspaceId = params.workspaceId;
    }

    if (formType === "destination") {
      if ("selectedConnectorId" in params) {
        payload.destinationId = params.selectedConnectorId;
      }

      if ("selectedConnectorDefinitionId" in params) {
        payload.destinationDefinitionId = params.selectedConnectorDefinitionId;
      }

      return await destinationService.check_connection(payload, {
        signal: params.signal,
      });
    }

    if ("selectedConnectorId" in params) {
      payload.sourceId = params.selectedConnectorId;
    }

    if ("selectedConnectorDefinitionId" in params) {
      payload.sourceDefinitionId = params.selectedConnectorDefinitionId;
    }

    return await sourceService.check_connection(payload, {
      signal: params.signal,
    });
  });
};
