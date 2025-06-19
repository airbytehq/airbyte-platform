import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useCallback, useEffect, useState } from "react";
import { flushSync } from "react-dom";
import { useIntl } from "react-intl";

import { ConnectionConfiguration } from "area/connector/types";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { isDefined } from "core/utils/common";

import { useRemoveConnectionsFromList } from "./connections";
import { useCurrentWorkspace } from "./workspaces";
import { ErrorWithJobInfo } from "../errors";
import {
  createSource,
  deleteSource,
  discoverSchemaForSource,
  getSource,
  listSourcesForWorkspace,
  updateSource,
} from "../generated/AirbyteClient";
import { SCOPE_WORKSPACE } from "../scopes";
import {
  AirbyteCatalog,
  ScopedResourceRequirements,
  SourceRead,
  WebBackendConnectionListItem,
} from "../types/AirbyteClient";
import { useRequestErrorHandler } from "../useRequestErrorHandler";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

export const sourcesKeys = {
  all: [SCOPE_WORKSPACE, "sources"] as const,
  lists: () => [...sourcesKeys.all, "list"] as const,
  list: (filters: string) => [...sourcesKeys.lists(), { filters }] as const,
  detail: (sourceId: string) => [...sourcesKeys.all, "details", sourceId] as const,
  discoverSchema: (sourceId: string) => [...sourcesKeys.all, "discoverSchema", sourceId] as const,
};

interface ValuesProps {
  name: string;
  serviceType?: string;
  connectionConfiguration: ConnectionConfiguration;
  resourceAllocation?: ScopedResourceRequirements;
}

interface ConnectorProps {
  name: string;
  sourceDefinitionId: string;
}

interface SourceList {
  sources: SourceRead[];
}

const useSourceList = (): SourceList => {
  const requestOptions = useRequestOptions();
  const workspace = useCurrentWorkspace();

  return useSuspenseQuery(sourcesKeys.lists(), () =>
    listSourcesForWorkspace({ workspaceId: workspace.workspaceId }, requestOptions)
  );
};

const useGetSource = <T extends string | undefined | null>(
  sourceId: T
): T extends string ? SourceRead : SourceRead | undefined => {
  const requestOptions = useRequestOptions();

  return useSuspenseQuery(
    sourcesKeys.detail(sourceId ?? ""),
    () => getSource({ sourceId: sourceId ?? "" }, requestOptions),
    {
      enabled: isDefined(sourceId),
    }
  );
};

export const useInvalidateSource = <T extends string | undefined | null>(sourceId: T): (() => void) => {
  const queryClient = useQueryClient();

  return useCallback(() => {
    queryClient.invalidateQueries(sourcesKeys.detail(sourceId ?? ""));
  }, [queryClient, sourceId]);
};

const useCreateSource = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const workspace = useCurrentWorkspace();
  const onError = useRequestErrorHandler("sources.createError");

  return useMutation(
    async (createSourcePayload: { values: ValuesProps; sourceConnector: ConnectorProps }) => {
      const { values, sourceConnector } = createSourcePayload;
      try {
        // Try to create source
        const result = await createSource(
          {
            name: values.name,
            sourceDefinitionId: sourceConnector?.sourceDefinitionId,
            workspaceId: workspace.workspaceId,
            connectionConfiguration: values.connectionConfiguration,
            resourceAllocation: values.resourceAllocation,
          },
          requestOptions
        );

        return result;
      } catch (e) {
        throw e;
      }
    },
    {
      onSuccess: (data) => {
        queryClient.setQueryData(sourcesKeys.lists(), (lst: SourceList | undefined) => ({
          sources: [data, ...(lst?.sources ?? [])],
        }));
      },
      onError,
    }
  );
};

const useDeleteSource = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const analyticsService = useAnalyticsService();
  const removeConnectionsFromList = useRemoveConnectionsFromList();
  const onError = useRequestErrorHandler("sources.deleteError");

  return useMutation(
    (payload: { source: SourceRead; connectionsWithSource: WebBackendConnectionListItem[] }) =>
      deleteSource({ sourceId: payload.source.sourceId }, requestOptions),
    {
      onSuccess: (_data, ctx) => {
        analyticsService.track(Namespace.SOURCE, Action.DELETE, {
          actionDescription: "Source deleted",
          connector_source: ctx.source.sourceName,
          connector_source_definition_id: ctx.source.sourceDefinitionId,
        });

        queryClient.removeQueries(sourcesKeys.detail(ctx.source.sourceId));
        queryClient.setQueryData(
          sourcesKeys.lists(),
          (lst: SourceList | undefined) =>
            ({
              sources: lst?.sources.filter((conn) => conn.sourceId !== ctx.source.sourceId) ?? [],
            }) as SourceList
        );

        const connectionIds = ctx.connectionsWithSource.map((item) => item.connectionId);
        removeConnectionsFromList(connectionIds);
      },
      onError,
    }
  );
};

const useUpdateSource = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const onError = useRequestErrorHandler("sources.updateError");

  return useMutation(
    (updateSourcePayload: { values: ValuesProps; sourceId: string }) => {
      return updateSource(
        {
          name: updateSourcePayload.values.name,
          sourceId: updateSourcePayload.sourceId,
          connectionConfiguration: updateSourcePayload.values.connectionConfiguration,
          resourceAllocation: updateSourcePayload.values.resourceAllocation,
        },
        requestOptions
      );
    },
    {
      onSuccess: (data) => {
        queryClient.setQueryData(sourcesKeys.detail(data.sourceId), data);
      },
      onError,
    }
  );
};

export const useDiscoverSchemaQuery = (sourceId: string) => {
  const requestOptions = useRequestOptions();

  return useQuery(
    sourcesKeys.discoverSchema(sourceId),
    async () => {
      return discoverSchemaForSource({ sourceId, disable_cache: true }, requestOptions);
    },
    {
      cacheTime: 0, // As soon as the query is not used, it should be removed from the cache
      staleTime: 1000 * 60 * 20, // A discovered schema should be valid for max 20 minutes on the client before refetching
    }
  );
};

const useDiscoverSchema = (
  sourceId: string,
  disableCache?: boolean
): {
  isLoading: boolean;
  schema: AirbyteCatalog | undefined;
  schemaErrorStatus: Error | null;
  catalogId: string | undefined;
  onDiscoverSchema: () => Promise<void>;
} => {
  const { formatMessage } = useIntl();
  const requestOptions = useRequestOptions();
  const [schema, setSchema] = useState<AirbyteCatalog | undefined>(undefined);
  const [catalogId, setCatalogId] = useState<string | undefined>("");
  const [isLoading, setIsLoading] = useState(false);
  const [schemaErrorStatus, setSchemaErrorStatus] = useState<Error | null>(null);

  const onDiscoverSchema = useCallback(async () => {
    setIsLoading(true);
    setSchemaErrorStatus(null);
    try {
      const result = await discoverSchemaForSource(
        { sourceId: sourceId || "", disable_cache: disableCache },
        requestOptions
      );

      if (!result.jobInfo?.succeeded) {
        throw new ErrorWithJobInfo(formatMessage({ id: "connector.discoverSchema.jobFailed" }), result.jobInfo);
      }
      if (!result.catalog) {
        throw new ErrorWithJobInfo(formatMessage({ id: "connector.discoverSchema.catalogMissing" }), result.jobInfo);
      }

      flushSync(() => {
        setSchema(result.catalog);
        setCatalogId(result.catalogId);
      });
    } catch (e) {
      setSchemaErrorStatus(e);
    } finally {
      setIsLoading(false);
    }
  }, [disableCache, formatMessage, requestOptions, sourceId]);

  useEffect(() => {
    if (sourceId) {
      onDiscoverSchema();
    }
  }, [onDiscoverSchema, sourceId]);

  return { schemaErrorStatus, isLoading, schema, catalogId, onDiscoverSchema };
};

export { useSourceList, useGetSource, useCreateSource, useDeleteSource, useUpdateSource, useDiscoverSchema };
