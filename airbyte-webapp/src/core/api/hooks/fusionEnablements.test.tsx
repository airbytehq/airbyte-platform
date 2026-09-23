import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import { ReactNode } from "react";

import {
  fusionEnablementKeys,
  useEnableFusionWorkspaceActors,
  useFusionActorEnablement,
  useSetFusionActorEnablement,
  useFusionWorkspaceConnectors,
} from "./fusionEnablements";
import {
  getFusionDestinationEnablement,
  getFusionSourceEnablement,
  listDestinationsForWorkspace,
  listSourcesForWorkspace,
  updateFusionDestinationEnablement,
  updateFusionSourceEnablement,
} from "../generated/AirbyteClient";

jest.mock("area/organization/utils", () => ({ useCurrentOrganizationId: () => "org" }));
jest.mock("../useRequestOptions", () => ({ useRequestOptions: () => ({ getAccessToken: async () => "token" }) }));
jest.mock("./agentsPlatform", () => ({
  useAgentsSupportedSourceDefinitionIds: () => new Set(["supported"]),
  useAgentsSupportedDestinationDefinitionIds: () => new Set(["supported"]),
}));
jest.mock("../generated/AirbyteClient", () => ({
  getFusionSourceEnablement: jest.fn(),
  getFusionDestinationEnablement: jest.fn(),
  updateFusionSourceEnablement: jest.fn(),
  updateFusionDestinationEnablement: jest.fn(),
  listSourcesForWorkspace: jest.fn(),
  listDestinationsForWorkspace: jest.fn(),
}));

const actor = { actorId: "source", actorKind: "source" as const, workspaceId: "workspace" };
const basic = { enable_agent_access: true, enable_indexing: false };
const disabled = { enable_agent_access: false, enable_indexing: false };
const state = { organizationId: "org", workspaceId: "workspace", actorId: "source", actorType: "source", ...basic };
const destination = { actorId: "destination", actorKind: "destination" as const, workspaceId: "workspace" };
const destinationFlags = {
  enable_agent_access: true,
  enable_indexing: true,
  enable_backfill: true,
  backfill_start_time: "2026-09-01T02:00:00.123456+02:00",
};
let client: QueryClient;
const wrapper = ({ children }: { children: ReactNode }) => (
  <QueryClientProvider client={client}>{children}</QueryClientProvider>
);
beforeEach(() => {
  jest.clearAllMocks();
  client = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
    logger: { log: jest.fn(), warn: jest.fn(), error: jest.fn() },
  });
  jest.mocked(getFusionSourceEnablement).mockResolvedValue(state as never);
  jest.mocked(updateFusionSourceEnablement).mockResolvedValue(state as never);
  const destinationRead = { ...state, actorId: "destination", actorType: "destination", ...destinationFlags };
  jest.mocked(getFusionDestinationEnablement).mockResolvedValue(destinationRead as never);
  jest.mocked(updateFusionDestinationEnablement).mockResolvedValue(destinationRead as never);
});
afterEach(() => client.clear());

it("deduplicates actor reads by organization workspace kind and actor", async () => {
  const { result } = renderHook(() => [useFusionActorEnablement(actor), useFusionActorEnablement(actor)], { wrapper });
  await waitFor(() => expect(result.current[0].data?.enable_agent_access).toBe(true));
  expect(getFusionSourceEnablement).toHaveBeenCalledTimes(1);
  expect(client.getQueryData(fusionEnablementKeys.actor("org", actor))).toEqual(state);
});

it("rejects cross-organization responses", async () => {
  jest.mocked(getFusionSourceEnablement).mockResolvedValue({ ...state, organizationId: "other" } as never);
  const { result } = renderHook(() => useFusionActorEnablement(actor), { wrapper });
  await waitFor(() => expect(result.current.isError).toBe(true));
  expect(result.current.data).toBeUndefined();
});

it("uses displayed expectedState and refetches committed revocations after failure", async () => {
  jest.mocked(updateFusionSourceEnablement).mockRejectedValue(new Error("sync failed"));
  const { result } = renderHook(
    () => ({ read: useFusionActorEnablement(actor), write: useSetFusionActorEnablement() }),
    { wrapper }
  );
  await waitFor(() => expect(result.current.read.isSuccess).toBe(true));
  jest.mocked(getFusionSourceEnablement).mockResolvedValue({ ...state, ...disabled } as never);
  await act(async () => {
    await expect(result.current.write.mutateAsync({ ...actor, state: disabled, expectedState: basic })).rejects.toThrow(
      "sync failed"
    );
  });
  await waitFor(() => expect(result.current.read.data?.enable_agent_access).toBe(false));
  expect(updateFusionSourceEnablement).toHaveBeenCalledWith(
    "source",
    { ...disabled, expectedState: basic },
    expect.anything()
  );
});

it("basic opt-in preserves existing indexing without implicitly adding it", async () => {
  const { result } = renderHook(() => useSetFusionActorEnablement(), { wrapper });
  await act(async () => {
    await result.current.mutateAsync({ ...actor, enabled: true });
  });
  expect(updateFusionSourceEnablement).toHaveBeenLastCalledWith(
    "source",
    { ...basic, expectedState: basic },
    expect.anything()
  );
  jest.mocked(getFusionSourceEnablement).mockResolvedValue({ ...state, enable_indexing: true } as never);
  await act(async () => {
    await result.current.mutateAsync({ ...actor, enabled: true });
  });
  expect(updateFusionSourceEnablement).toHaveBeenLastCalledWith(
    "source",
    { ...basic, enable_indexing: true, expectedState: { ...basic, enable_indexing: true } },
    expect.anything()
  );
});

it("destination access disable retains exact timestamp while disabling both dependent flags", async () => {
  const { result } = renderHook(() => useSetFusionActorEnablement(), { wrapper });
  await act(async () => {
    await result.current.mutateAsync({ ...destination, enabled: false, expectedState: destinationFlags });
  });
  expect(updateFusionDestinationEnablement).toHaveBeenCalledWith(
    "destination",
    {
      ...disabled,
      enable_backfill: false,
      backfill_start_time: destinationFlags.backfill_start_time,
      expectedState: destinationFlags,
    },
    expect.anything()
  );
  expect(getFusionDestinationEnablement).not.toHaveBeenCalled();
});

it("nullable timestamp clearing is explicit and preserves the exact prior timestamp in CAS", async () => {
  const { result } = renderHook(() => useSetFusionActorEnablement(), { wrapper });
  await act(async () => {
    await result.current.mutateAsync({
      ...destination,
      state: { ...destinationFlags, backfill_start_time: null },
      expectedState: destinationFlags,
    });
  });
  expect(updateFusionDestinationEnablement).toHaveBeenCalledWith(
    "destination",
    { ...destinationFlags, backfill_start_time: null, expectedState: destinationFlags },
    expect.anything()
  );
});

it("bulk returns failed actors and retry neither inventories nor reenables successful actors", async () => {
  jest.mocked(listSourcesForWorkspace).mockResolvedValue({
    sources: ["source", "source2"].map((sourceId) => ({ sourceId, sourceDefinitionId: "supported" })),
  } as never);
  jest.mocked(listDestinationsForWorkspace).mockResolvedValue({ destinations: [] });
  jest.mocked(getFusionSourceEnablement).mockImplementation(async (actorId) => ({ ...state, actorId }) as never);
  jest
    .mocked(updateFusionSourceEnablement)
    .mockRejectedValueOnce(new Error("403"))
    .mockResolvedValue(state as never);
  const { result } = renderHook(() => useEnableFusionWorkspaceActors(), { wrapper });
  let failed: Array<typeof actor> = [];
  await act(async () => {
    failed = (await result.current.mutateAsync({ workspaceIds: ["workspace"] })) as Array<typeof actor>;
  });
  expect(failed).toEqual([actor]);
  expect(updateFusionSourceEnablement).toHaveBeenCalledTimes(2);
  await act(async () => {
    expect(await result.current.mutateAsync({ workspaceIds: ["workspace"], retryActors: failed })).toEqual([]);
  });
  expect(updateFusionSourceEnablement).toHaveBeenCalledTimes(3);
  expect(listSourcesForWorkspace).toHaveBeenCalledTimes(1);
});

it("empty bulk selections perform no writes", async () => {
  const { result } = renderHook(() => useEnableFusionWorkspaceActors(), { wrapper });
  await act(async () => {
    expect(await result.current.mutateAsync({ workspaceIds: [] })).toEqual([]);
  });
  expect(updateFusionSourceEnablement).not.toHaveBeenCalled();
});

it.each([true, false])("destination opt-in enables indexing with displayed state=%s", async (displayed) => {
  const expected = { ...destinationFlags, ...disabled, enable_backfill: false };
  jest.mocked(getFusionDestinationEnablement).mockResolvedValue({
    ...state,
    actorId: "destination",
    actorType: "destination",
    ...expected,
  } as never);
  const { result } = renderHook(() => useSetFusionActorEnablement(), { wrapper });
  await act(async () => {
    await result.current.mutateAsync({
      ...destination,
      enabled: true,
      ...(displayed ? { expectedState: expected } : {}),
    });
  });
  expect(updateFusionDestinationEnablement).toHaveBeenLastCalledWith(
    "destination",
    {
      ...expected,
      enable_agent_access: true,
      enable_indexing: true,
      expectedState: expected,
    },
    expect.anything()
  );
});

it("enrollment enables access and indexing for a disabled supported destination", async () => {
  jest.mocked(listSourcesForWorkspace).mockResolvedValue({ sources: [] });
  const inventory = { destinations: [{ destinationId: "destination", destinationDefinitionId: "supported" }] };
  jest.mocked(listDestinationsForWorkspace).mockResolvedValue(inventory as never);
  const expected = { ...destinationFlags, ...disabled, enable_backfill: false };
  jest.mocked(getFusionDestinationEnablement).mockResolvedValue({
    ...state,
    actorId: "destination",
    actorType: "destination",
    ...expected,
  } as never);
  const { result } = renderHook(() => useEnableFusionWorkspaceActors(), { wrapper });
  await act(async () => {
    expect(await result.current.mutateAsync({ workspaceIds: ["workspace"] })).toEqual([]);
  });
  expect(updateFusionDestinationEnablement).toHaveBeenLastCalledWith(
    "destination",
    {
      ...expected,
      enable_agent_access: true,
      enable_indexing: true,
      expectedState: expected,
    },
    expect.anything()
  );
});

it.each(["source", "destination"] as const)(
  "keeps the other inventory usable when %s inventory fails and recovers",
  async (failedKind) => {
    const sources = { sources: [{ sourceId: "source", name: "Source", sourceDefinitionId: "supported" }] };
    const destinations = {
      destinations: [{ destinationId: "destination", name: "Destination", destinationDefinitionId: "supported" }],
    };
    jest.mocked(listSourcesForWorkspace).mockResolvedValue(sources as never);
    jest.mocked(listDestinationsForWorkspace).mockResolvedValue(destinations as never);
    const failingList = failedKind === "source" ? listSourcesForWorkspace : listDestinationsForWorkspace;
    jest.mocked(failingList).mockRejectedValueOnce(new Error("inventory unavailable"));
    const { result } = renderHook(() => useFusionWorkspaceConnectors("workspace"), { wrapper });
    const healthyKind = failedKind === "source" ? "destinations" : "sources";
    await waitFor(() => {
      expect(result.current.isLoading).toBe(false);
      expect(result.current.sourcesError).toBe(failedKind === "source");
      expect(result.current.destinationsError).toBe(failedKind === "destination");
      expect(result.current[healthyKind][0].state?.enable_agent_access).toBe(true);
    });
    expect(result.current[healthyKind][0].error).toBe(false);
    await act(async () => {
      await client.refetchQueries({ type: "active" });
    });
    await waitFor(() => {
      expect(result.current.sourcesError).toBe(false);
      expect(result.current.destinationsError).toBe(false);
      expect(result.current.sources[0].state?.enable_agent_access).toBe(true);
      expect(result.current.destinations[0].state?.enable_agent_access).toBe(true);
    });
  }
);
