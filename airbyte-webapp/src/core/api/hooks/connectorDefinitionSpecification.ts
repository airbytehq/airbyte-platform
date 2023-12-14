import { useQuery } from "@tanstack/react-query";

import { isDefined } from "core/utils/common";

import { useCurrentWorkspace } from "./workspaces";
import {
  getDestinationDefinitionSpecification,
  getSourceDefinitionSpecification,
  getSpecificationForDestinationId,
  getSpecificationForSourceId,
} from "../generated/AirbyteClient";
import { SCOPE_WORKSPACE } from "../scopes";
import { DestinationDefinitionSpecificationRead } from "../types/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

export const destinationDefinitionSpecificationKeys = {
  all: [SCOPE_WORKSPACE, "destinationDefinitionSpecification"] as const,
  detail: (destinationDefId: string | number, destinationId?: string) =>
    [...destinationDefinitionSpecificationKeys.all, "details", { destinationDefId, destinationId }] as const,
};

export const sourceDefinitionSpecificationKeys = {
  all: [SCOPE_WORKSPACE, "sourceDefinitionSpecification"] as const,
  detail: (sourceDefId: string | number, sourceId?: string) =>
    [...sourceDefinitionSpecificationKeys.all, "details", { sourceDefId, sourceId }] as const,
};

export const useGetDestinationDefinitionSpecification = (
  destinationDefinitionId: string,
  destinationId?: string
): DestinationDefinitionSpecificationRead => {
  const requestOptions = useRequestOptions();
  const { workspaceId } = useCurrentWorkspace();
  return useSuspenseQuery(destinationDefinitionSpecificationKeys.detail(destinationDefinitionId, destinationId), () => {
    if (destinationId) {
      return getSpecificationForDestinationId({ destinationId }, requestOptions);
    }

    return getDestinationDefinitionSpecification({ destinationDefinitionId, workspaceId }, requestOptions);
  });
};

export const useGetDestinationDefinitionSpecificationAsync = (id: string | null) => {
  const requestOptions = useRequestOptions();
  const { workspaceId } = useCurrentWorkspace();

  const escapedId = id ?? "";
  return useQuery(
    destinationDefinitionSpecificationKeys.detail(escapedId),
    () => getDestinationDefinitionSpecification({ destinationDefinitionId: escapedId, workspaceId }, requestOptions),
    {
      enabled: isDefined(id),
    }
  );
};

export const useGetSourceDefinitionSpecification = (sourceDefinitionId: string, sourceId?: string) => {
  const requestOptions = useRequestOptions();
  const { workspaceId } = useCurrentWorkspace();
  return useSuspenseQuery(sourceDefinitionSpecificationKeys.detail(sourceDefinitionId, sourceId), () => {
    if (sourceId) {
      return getSpecificationForSourceId({ sourceId }, requestOptions);
    }

    return getSourceDefinitionSpecification({ sourceDefinitionId, workspaceId }, requestOptions);
  });
};

export const useGetSourceDefinitionSpecificationAsync = (id: string | null) => {
  const requestOptions = useRequestOptions();

  const { workspaceId } = useCurrentWorkspace();

  const escapedId = id ?? "";
  return useQuery(
    sourceDefinitionSpecificationKeys.detail(escapedId),
    () => getSourceDefinitionSpecification({ sourceDefinitionId: escapedId, workspaceId }, requestOptions),
    {
      enabled: isDefined(id),
    }
  );
};
