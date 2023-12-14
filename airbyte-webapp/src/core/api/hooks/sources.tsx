import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useCallback, useEffect, useState } from "react";
import { flushSync } from "react-dom";

import { ConnectionConfiguration } from "area/connector/types";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { isDefined } from "core/utils/common";

import { useRemoveConnectionsFromList } from "./connections";
import { useCurrentWorkspace } from "./workspaces";
import {
  createSource,
  deleteSource,
  discoverSchemaForSource,
  getSource,
  listSourcesForWorkspace,
  updateSource,
} from "../generated/AirbyteClient";
import { SCOPE_WORKSPACE } from "../scopes";
import { AirbyteCatalog, SourceRead, SynchronousJobRead, WebBackendConnectionListItem } from "../types/AirbyteClient";
import { useRequestErrorHandler } from "../useRequestErrorHandler";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

export const sourcesKeys = {
  all: [SCOPE_WORKSPACE, "sources"] as const,
  lists: () => [...sourcesKeys.all, "list"] as const,
  list: (filters: string) => [...sourcesKeys.lists(), { filters }] as const,
  detail: (sourceId: string) => [...sourcesKeys.all, "details", sourceId] as const,
};

interface ValuesProps {
  name: string;
  serviceType?: string;
  connectionConfiguration?: ConnectionConfiguration;
  frequency?: string;
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

export type SchemaError = (Error & { status: number; response: SynchronousJobRead }) | null;

const useDiscoverSchema = (
  sourceId: string,
  disableCache?: boolean
): {
  isLoading: boolean;
  schema: AirbyteCatalog | undefined;
  schemaErrorStatus: SchemaError;
  catalogId: string | undefined;
  onDiscoverSchema: () => Promise<void>;
} => {
  const requestOptions = useRequestOptions();
  const [schema, setSchema] = useState<AirbyteCatalog | undefined>(undefined);
  const [catalogId, setCatalogId] = useState<string | undefined>("");
  const [isLoading, setIsLoading] = useState(false);
  const [schemaErrorStatus, setSchemaErrorStatus] = useState<SchemaError>(null);

  const onDiscoverSchema = useCallback(async () => {
    setIsLoading(true);
    setSchemaErrorStatus(null);
    try {
      const result = await discoverSchemaForSource(
        { sourceId: sourceId || "", disable_cache: disableCache },
        requestOptions
      );

      if (!result.jobInfo?.succeeded || !result.catalog) {
        // @ts-expect-error TODO: address this case
        const e = result.jobInfo?.logs ? new LogsRequestError(result.jobInfo) : new CommonRequestError(result);
        // Generate error with failed status and received logs
        e._status = 400;
        e.response = result.jobInfo;
        throw e;
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
  }, [disableCache, requestOptions, sourceId]);

  useEffect(() => {
    if (sourceId) {
      onDiscoverSchema();
    }
  }, [onDiscoverSchema, sourceId]);

  return { schemaErrorStatus, isLoading, schema, catalogId, onDiscoverSchema };
};

export { useSourceList, useGetSource, useCreateSource, useDeleteSource, useUpdateSource, useDiscoverSchema };
