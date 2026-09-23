import { useIsMutating, useMutation, useQueries, useQuery, useQueryClient } from "@tanstack/react-query";

import { useCurrentOrganizationId } from "area/organization/utils";

import { useAgentsSupportedDestinationDefinitionIds, useAgentsSupportedSourceDefinitionIds } from "./agentsPlatform";
import { ApiCallOptions } from "../apiCall";
import {
  getFusionDestinationEnablement,
  getFusionSourceEnablement,
  listDestinationsForWorkspace,
  listSourcesForWorkspace,
  updateFusionDestinationEnablement,
  updateFusionSourceEnablement,
} from "../generated/AirbyteClient";
import {
  FusionSourceEnablementRead,
  FusionDestinationEnablementRead,
  FusionSourceEnablementState,
  FusionDestinationEnablementState,
} from "../generated/AirbyteClient.schemas";
import { SCOPE_ORGANIZATION } from "../scopes";
import { useRequestOptions } from "../useRequestOptions";

export type FusionEnablementRead = FusionSourceEnablementRead | FusionDestinationEnablementRead;
export type FusionEnablementState = FusionSourceEnablementState | FusionDestinationEnablementState;

// Extract only state fields: read identities are not accepted in a Config API write.
export const fusionEnablementState = (read: FusionEnablementRead): FusionEnablementState =>
  "enable_backfill" in read
    ? {
        enable_agent_access: read.enable_agent_access,
        enable_indexing: read.enable_indexing,
        enable_backfill: read.enable_backfill,
        backfill_start_time: read.backfill_start_time ?? null,
      }
    : { enable_agent_access: read.enable_agent_access, enable_indexing: read.enable_indexing };

type ActorKind = "source" | "destination";
export interface FusionActor {
  actorId: string;
  actorKind: ActorKind;
  workspaceId: string;
}
export const fusionEnablementKeys = {
  all: (organizationId: string) => [SCOPE_ORGANIZATION, "fusionEnablements", organizationId] as const,
  actor: (organizationId: string, actor: FusionActor) =>
    [...fusionEnablementKeys.all(organizationId), actor.workspaceId, actor.actorKind, actor.actorId] as const,
};

// Shared by table cells, workspace hydration, and post-enrollment reconciliation.
let activeReads = 0;
const waitingReads: Array<() => void> = [];
const readEnablements = async (
  organizationId: string,
  actor: FusionActor,
  options: ApiCallOptions
): Promise<FusionEnablementRead> => {
  if (activeReads >= 4) {
    await new Promise<void>((resolve) => waitingReads.push(resolve));
  } else {
    activeReads++;
  }
  try {
    const state = await (actor.actorKind === "source"
      ? getFusionSourceEnablement(actor.actorId, options)
      : getFusionDestinationEnablement(actor.actorId, options));
    if (
      state.organizationId !== organizationId ||
      state.workspaceId !== actor.workspaceId ||
      state.actorId !== actor.actorId ||
      state.actorType !== actor.actorKind
    ) {
      throw new Error("Actor enablement identity mismatch");
    }
    return state;
  } finally {
    const next = waitingReads.shift();
    if (next) {
      next();
    } else {
      activeReads--;
    }
  }
};

export const useFusionActorEnablement = (actor: FusionActor, enabled = true) => {
  const organizationId = useCurrentOrganizationId();
  const options = useRequestOptions();
  return useQuery(
    fusionEnablementKeys.actor(organizationId, actor),
    () => readEnablements(organizationId, actor, options),
    { enabled, retry: false, staleTime: 30_000 }
  );
};

export const useFusionActorSaving = (actor: FusionActor) => {
  const organizationId = useCurrentOrganizationId();
  return (
    useIsMutating({
      mutationKey: fusionEnablementKeys.all(organizationId),
      predicate: (mutation) => {
        const variables = mutation.state.variables as FusionActor | undefined;
        return (
          variables?.actorId === actor.actorId &&
          variables.actorKind === actor.actorKind &&
          variables.workspaceId === actor.workspaceId
        );
      },
    }) > 0
  );
};

type Mutation = FusionActor & { expectedState?: FusionEnablementState } & (
    | { state: FusionEnablementState; enabled?: never }
    | { enabled: boolean; state?: never }
  );
export const useSetFusionActorEnablement = () => {
  const organizationId = useCurrentOrganizationId();
  const options = useRequestOptions();
  const queryClient = useQueryClient();
  return useMutation(
    async (actor: Mutation) => {
      // Creation and reconciliation callers read fresh state; interactive toggles supply their displayed CAS state.
      const expected =
        actor.expectedState ?? fusionEnablementState(await readEnablements(organizationId, actor, options));
      const desired = actor.state ?? {
        ...expected,
        enable_agent_access: actor.enabled,
        enable_indexing: actor.enabled && (actor.actorKind === "destination" || expected.enable_indexing),
        ...("enable_backfill" in expected ? { enable_backfill: actor.enabled && expected.enable_backfill } : {}),
      };
      const state =
        actor.actorKind === "source"
          ? await updateFusionSourceEnablement(
              actor.actorId,
              {
                enable_agent_access: desired.enable_agent_access,
                enable_indexing: desired.enable_indexing,
                expectedState: {
                  enable_agent_access: expected.enable_agent_access,
                  enable_indexing: expected.enable_indexing,
                },
              },
              options
            )
          : await updateFusionDestinationEnablement(
              actor.actorId,
              {
                enable_agent_access: desired.enable_agent_access,
                enable_indexing: desired.enable_indexing,
                enable_backfill: "enable_backfill" in desired ? desired.enable_backfill : false,
                backfill_start_time: "backfill_start_time" in desired ? desired.backfill_start_time ?? null : null,
                expectedState: {
                  enable_agent_access: expected.enable_agent_access,
                  enable_indexing: expected.enable_indexing,
                  enable_backfill: "enable_backfill" in expected ? expected.enable_backfill : false,
                  backfill_start_time: "backfill_start_time" in expected ? expected.backfill_start_time ?? null : null,
                },
              },
              options
            );
      queryClient.setQueryData(fusionEnablementKeys.actor(organizationId, actor), state);
      return {
        ...state,
        enabled: state.enable_agent_access,
      };
    },
    {
      mutationKey: fusionEnablementKeys.all(organizationId),
      onSettled: async (_data, _error, actor) => {
        await queryClient.invalidateQueries(fusionEnablementKeys.actor(organizationId, actor));
      },
    }
  );
};

export interface FusionWorkspaceConnector {
  id: string;
  name: string;
  supported: boolean;
  enabled: boolean;
  state?: FusionEnablementState;
  loading?: boolean;
  error?: boolean;
}
export const useFusionWorkspaceConnectors = (
  workspaceId: string,
  { enabled = true, hydrate = true }: { enabled?: boolean; hydrate?: boolean } = {}
): {
  sources: FusionWorkspaceConnector[];
  destinations: FusionWorkspaceConnector[];
  isLoading: boolean;
  sourcesError: boolean;
  destinationsError: boolean;
} => {
  const organizationId = useCurrentOrganizationId();
  const options = useRequestOptions();
  const supportedSources = useAgentsSupportedSourceDefinitionIds();
  const supportedDestinations = useAgentsSupportedDestinationDefinitionIds();
  const inventories = useQueries({
    queries: (["source", "destination"] as const).map((kind) => ({
      queryKey: [SCOPE_ORGANIZATION, "fusionInventory", organizationId, workspaceId, kind],
      queryFn: async () => {
        const actors: Array<FusionWorkspaceConnector & { kind: ActorKind }> = [];
        let cursor: string | undefined;
        while (true) {
          const page =
            kind === "source"
              ? (await listSourcesForWorkspace({ workspaceId, pageSize: 100, cursor }, options)).sources.map(
                  (source) => ({
                    id: source.sourceId,
                    name: source.name,
                    supported: supportedSources.has(source.sourceDefinitionId),
                  })
                )
              : (await listDestinationsForWorkspace({ workspaceId, pageSize: 100, cursor }, options)).destinations.map(
                  (destination) => ({
                    id: destination.destinationId,
                    name: destination.name,
                    supported: supportedDestinations.has(destination.destinationDefinitionId),
                  })
                );
          actors.push(...page.map((actor) => ({ ...actor, kind, enabled: false })));
          if (page.length < 100) {
            break;
          }
          cursor = page.at(-1)?.id;
        }
        return actors;
      },
      enabled,
      retry: false,
      staleTime: 30_000,
    })),
  });
  const actors = inventories.flatMap((inventory) => inventory.data ?? []);
  const states = useQueries({
    queries: (hydrate ? actors : []).map((actor) => ({
      queryKey: fusionEnablementKeys.actor(organizationId, { workspaceId, actorId: actor.id, actorKind: actor.kind }),
      queryFn: () =>
        readEnablements(organizationId, { workspaceId, actorId: actor.id, actorKind: actor.kind }, options),
      enabled: enabled && hydrate && actor.supported,
      retry: false,
      staleTime: 30_000,
    })),
  });
  const connectors = actors.map((actor, index) => ({
    ...actor,
    state: states[index]?.data ? fusionEnablementState(states[index].data!) : undefined,
    enabled: states[index]?.data?.enable_agent_access ?? false,
    loading: hydrate && actor.supported && states[index]?.isLoading,
    error: states[index]?.isError,
  }));
  return {
    sources: connectors.filter((actor) => actor.kind === "source"),
    destinations: connectors.filter((actor) => actor.kind === "destination"),
    isLoading: inventories.some((inventory) => inventory.isLoading),
    sourcesError: inventories[0].isError,
    destinationsError: inventories[1].isError,
  };
};

export const useEnableFusionWorkspaceActors = () => {
  const options = useRequestOptions();
  const supportedSources = useAgentsSupportedSourceDefinitionIds();
  const supportedDestinations = useAgentsSupportedDestinationDefinitionIds();
  const mutation = useSetFusionActorEnablement();
  return useMutation(async ({ workspaceIds, retryActors }: { workspaceIds: string[]; retryActors?: FusionActor[] }) => {
    const actors = retryActors ? [...retryActors] : [];
    if (!retryActors) {
      for (const workspaceId of workspaceIds) {
        for (const actorKind of ["source", "destination"] as const) {
          let cursor: string | undefined;
          while (true) {
            const page =
              actorKind === "source"
                ? (await listSourcesForWorkspace({ workspaceId, pageSize: 100, cursor }, options)).sources.map(
                    (actor) => ({ id: actor.sourceId, supported: supportedSources.has(actor.sourceDefinitionId) })
                  )
                : (
                    await listDestinationsForWorkspace({ workspaceId, pageSize: 100, cursor }, options)
                  ).destinations.map((actor) => ({
                    id: actor.destinationId,
                    supported: supportedDestinations.has(actor.destinationDefinitionId),
                  }));
            actors.push(
              ...page.filter((actor) => actor.supported).map((actor) => ({ workspaceId, actorKind, actorId: actor.id }))
            );
            if (page.length < 100) {
              break;
            }
            cursor = page.at(-1)?.id;
          }
        }
      }
    }
    const failed: FusionActor[] = [];
    // Keep bulk writes below the server's two-mutation admission bound.
    for (const actor of actors) {
      try {
        await mutation.mutateAsync({ ...actor, enabled: true });
      } catch {
        failed.push(actor);
      }
    }
    return failed;
  });
};
