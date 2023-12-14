import { isDefined } from "core/utils/common";

import {
  getActorDefinitionVersionForDestinationId,
  getActorDefinitionVersionForSourceId,
} from "../generated/AirbyteClient";
import { SCOPE_WORKSPACE } from "../scopes";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

export const definitionKeys = {
  all: [SCOPE_WORKSPACE, "actorDefinitionVersion"],
  detail: (connectorId: string) => [...definitionKeys.all, connectorId],
};

export function useSourceDefinitionVersion(sourceId?: string) {
  const requestOptions = useRequestOptions();

  return useSuspenseQuery(
    definitionKeys.detail(sourceId ?? ""),
    () => getActorDefinitionVersionForSourceId({ sourceId: sourceId ?? "" }, requestOptions),
    { enabled: isDefined(sourceId) }
  );
}

export function useDestinationDefinitionVersion(destinationId?: string) {
  const requestOptions = useRequestOptions();

  return useSuspenseQuery(
    definitionKeys.detail(destinationId ?? ""),
    () => getActorDefinitionVersionForDestinationId({ destinationId: destinationId ?? "" }, requestOptions),
    { enabled: isDefined(destinationId) }
  );
}
