import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { v4 as uuid } from "uuid";

import { useCurrentOrganizationId } from "area/organization/utils";
import { useCurrentWorkspaceId } from "area/workspace/utils";
import { isDefined } from "core/utils/common";
import { trackError } from "core/utils/datadog";

import { pollCommandUntilResolved } from "./commands";
import { connectorDefinitionKeys } from "./connectorUpdates";
import { useDefaultWorkspaceInOrganization } from "./organizations";
import {
  createCustomSourceDefinition,
  deleteSourceDefinition,
  getSourceDefinitionForWorkspace,
  getSpecCommandOutput,
  listLatestSourceDefinitions,
  listSourceDefinitionsForWorkspace,
  runSpecCommand,
  updateSourceDefinition,
} from "../generated/AirbyteClient";
import { SCOPE_WORKSPACE } from "../scopes";
import {
  SourceDefinitionCreate,
  SourceDefinitionIdRequestBody,
  SourceDefinitionRead,
  SourceDefinitionReadList,
} from "../types/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

export const sourceDefinitionKeys = {
  all: [SCOPE_WORKSPACE, "sourceDefinition"] as const,
  lists: (filterByUsed: boolean = false) => [...sourceDefinitionKeys.all, "list", filterByUsed] as const,
  listLatest: () => [...sourceDefinitionKeys.all, "listLatest"] as const,
  detail: (id: string) => [...sourceDefinitionKeys.all, "details", id] as const,
};

interface SourceDefinitions {
  sourceDefinitions: SourceDefinitionRead[];
  sourceDefinitionMap: Map<string, SourceDefinitionRead>;
}

export const useSourceDefinitionList = ({ filterByUsed }: { filterByUsed?: boolean } = {}): SourceDefinitions => {
  const requestOptions = useRequestOptions();
  const currentWorkspaceId = useCurrentWorkspaceId();
  const defaultWorkspaceId = useDefaultWorkspaceInOrganization(useCurrentOrganizationId());
  const workspaceId = currentWorkspaceId || defaultWorkspaceId?.workspaceId;

  return useQuery(
    sourceDefinitionKeys.lists(filterByUsed),
    async () => {
      const { sourceDefinitions } = await listSourceDefinitionsForWorkspace(
        { workspaceId: workspaceId || "", filterByUsed },
        requestOptions
      ).then(({ sourceDefinitions }) => ({
        sourceDefinitions: sourceDefinitions.sort((a, b) => a.name.localeCompare(b.name)),
      }));
      const sourceDefinitionMap = new Map<string, SourceDefinitionRead>();
      sourceDefinitions.forEach((sourceDefinition) => {
        sourceDefinitionMap.set(sourceDefinition.sourceDefinitionId, sourceDefinition);
      });
      return {
        sourceDefinitions,
        sourceDefinitionMap,
      };
    },
    { suspense: true, staleTime: Infinity }
  ).data as SourceDefinitions;
};

/**
 * This hook calls the list_latest endpoint, which is not needed under most circumstances. Only use this hook if you need the latest source definitions available, for example when prompting the user to update a connector version.
 */
export const useLatestSourceDefinitionList = (): SourceDefinitionReadList => {
  const requestOptions = useRequestOptions();
  return useSuspenseQuery(sourceDefinitionKeys.listLatest(), () => listLatestSourceDefinitions(requestOptions), {
    staleTime: 60_000,
  });
};

export const useSourceDefinition = <T extends string | undefined>(
  sourceDefinitionId: T
): T extends string ? SourceDefinitionRead : SourceDefinitionRead | undefined => {
  const requestOptions = useRequestOptions();
  const workspaceId = useCurrentWorkspaceId();

  return useSuspenseQuery(
    sourceDefinitionKeys.detail(sourceDefinitionId || ""),
    () =>
      getSourceDefinitionForWorkspace({ workspaceId, sourceDefinitionId: sourceDefinitionId || "" }, requestOptions),
    {
      enabled: isDefined(sourceDefinitionId),
    }
  );
};

export const useCreateSourceDefinition = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const workspaceId = useCurrentWorkspaceId();

  return useMutation<SourceDefinitionRead, Error, SourceDefinitionCreate>(
    (sourceDefinition) => createCustomSourceDefinition({ workspaceId, sourceDefinition }, requestOptions),
    {
      onSuccess: (data) => {
        queryClient.setQueryData(sourceDefinitionKeys.lists(), (oldData: SourceDefinitions | undefined) => {
          const newMap = new Map(oldData?.sourceDefinitionMap);
          newMap.set(data.sourceDefinitionId, data);
          return {
            sourceDefinitions: [data, ...(oldData?.sourceDefinitions ?? [])],
            sourceDefinitionMap: newMap,
          };
        });
      },
    }
  );
};

export const useCreateSourceDefinitionCommand = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const workspaceId = useCurrentWorkspaceId();

  return useMutation<SourceDefinitionRead, Error, SourceDefinitionCreate>(
    async (sourceDefinition) => {
      // 1. Run SPEC command asynchronously
      const commandId = uuid();
      await runSpecCommand(
        {
          id: commandId,
          workspace_id: workspaceId,
          docker_image: sourceDefinition.dockerRepository,
          docker_image_tag: sourceDefinition.dockerImageTag,
        },
        requestOptions
      );

      // 2. Poll until complete
      const status = await pollCommandUntilResolved(commandId, requestOptions);

      if (status === "cancelled") {
        throw new Error("Spec command was cancelled");
      }

      // 3. Get spec output
      const specOutput = await getSpecCommandOutput({ id: commandId }, requestOptions);

      if (specOutput.status !== "succeeded") {
        const errorMessage = specOutput.failureReason?.externalMessage || "Failed to fetch connector spec";
        throw new Error(errorMessage);
      }

      // 4. Create custom definition WITH the pre-fetched spec
      return createCustomSourceDefinition(
        {
          workspaceId,
          sourceDefinition: {
            ...sourceDefinition,
            connectorSpecification: specOutput.spec,
          },
        },
        requestOptions
      );
    },
    {
      onSuccess: (data) => {
        queryClient.setQueryData(sourceDefinitionKeys.lists(), (oldData: SourceDefinitions | undefined) => {
          const newMap = new Map(oldData?.sourceDefinitionMap);
          newMap.set(data.sourceDefinitionId, data);
          return {
            sourceDefinitions: [data, ...(oldData?.sourceDefinitions ?? [])],
            sourceDefinitionMap: newMap,
          };
        });
      },
    }
  );
};

export const useUpdateSourceDefinition = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const currentWorkspaceId = useCurrentWorkspaceId();
  const currentOrganizationId = useCurrentOrganizationId();
  const defaultWorkspace = useDefaultWorkspaceInOrganization(currentOrganizationId);

  return useMutation<
    SourceDefinitionRead,
    Error,
    {
      sourceDefinitionId: string;
      dockerImageTag: string;
    }
  >(
    (sourceDefinition) =>
      updateSourceDefinition(
        // Note: there is a possible edge case here where this method is called in an organization where all workspaces
        // have been deleted. In that case, both currentWorkspaceId and defaultWorkspace would be unset, which would
        // cause the API to fail. This is the best we can do unless the endpoint is changed to not require a workspaceId.
        { ...sourceDefinition, workspaceId: currentWorkspaceId || defaultWorkspace?.workspaceId || "" },
        requestOptions
      ),
    {
      onSuccess: (data) => {
        queryClient.setQueryData(sourceDefinitionKeys.detail(data.sourceDefinitionId), data);

        queryClient.setQueryData(sourceDefinitionKeys.lists(), (oldData: SourceDefinitions | undefined) => {
          const newMap = new Map(oldData?.sourceDefinitionMap);
          newMap.set(data.sourceDefinitionId, data);
          return {
            sourceDefinitions:
              oldData?.sourceDefinitions.map((sd) => (sd.sourceDefinitionId === data.sourceDefinitionId ? data : sd)) ??
              [],
            sourceDefinitionMap: newMap,
          };
        });

        queryClient.invalidateQueries(connectorDefinitionKeys.count());
      },
      onError: (error: Error) => {
        trackError(error);
      },
    }
  );
};

export const useDeleteSourceDefinition = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();

  const { mutateAsync } = useMutation(
    (body: SourceDefinitionIdRequestBody) => deleteSourceDefinition(body, requestOptions).then(() => body),
    {
      onSuccess: (body) => {
        queryClient.setQueryData(sourceDefinitionKeys.detail(body.sourceDefinitionId), undefined);
        queryClient.setQueryData(sourceDefinitionKeys.lists(), (oldData: SourceDefinitions | undefined) => {
          const newMap = new Map(oldData?.sourceDefinitionMap);
          newMap.delete(body.sourceDefinitionId);
          return {
            sourceDefinitions:
              oldData?.sourceDefinitions.filter(
                ({ sourceDefinitionId }) => sourceDefinitionId !== body.sourceDefinitionId
              ) ?? [],
            sourceDefinitionMap: newMap,
          };
        });
        queryClient.invalidateQueries(connectorDefinitionKeys.count());
      },
    }
  );

  return {
    deleteSourceDefinition: mutateAsync,
  };
};
