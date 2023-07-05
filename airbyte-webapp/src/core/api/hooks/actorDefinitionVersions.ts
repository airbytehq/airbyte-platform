import { SCOPE_WORKSPACE } from "services/Scope";

import {
  getActorDefinitionVersionForDestinationId,
  getActorDefinitionVersionForSourceId,
} from "../generated/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

export function useSourceDefinitionVersion(sourceId: string) {
  const requestOptions = useRequestOptions();

  return useSuspenseQuery([SCOPE_WORKSPACE, "actorDefinitionVersion", sourceId], () =>
    getActorDefinitionVersionForSourceId({ sourceId }, requestOptions)
  );
}

export function useDestinationDefinitionVersion(destinationId: string) {
  const requestOptions = useRequestOptions();

  return useSuspenseQuery([SCOPE_WORKSPACE, "actorDefinitionVersion", destinationId], () =>
    getActorDefinitionVersionForDestinationId({ destinationId }, requestOptions)
  );
}
