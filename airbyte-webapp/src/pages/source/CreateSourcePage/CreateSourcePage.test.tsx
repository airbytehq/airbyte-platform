import { act } from "@testing-library/react";

import { mockSource, render } from "test-utils";
import { mockExperiments } from "test-utils/mockExperiments";

import { useShowAgentsOptIn } from "cloud/components/AgentsOptIn/useShowAgentsOptIn";
import {
  useAgentsProvisioningStatus,
  useAgentsSupportedSourceDefinitionIds,
  useCreateSource,
  useGetSourceDefinitionSpecificationAsync,
  useSetFusionActorEnablement,
  useSourceDefinitionList,
  useUpdateSource,
} from "core/api";
import { useIsCloudApp } from "core/utils/app";
import { useGeneratedIntent } from "core/utils/rbac";

import { CreateSourcePage } from "./CreateSourcePage";

const mockSourceFormWithAgent = jest.fn((_props: unknown) => null);

jest.mock("./SourceFormWithAgent", () => ({
  SourceFormWithAgent: (props: unknown) => mockSourceFormWithAgent(props),
}));

jest.mock("components/ui/HeadTitle", () => ({ HeadTitle: () => null }));
jest.mock("cloud/components/AgentsOptIn", () => ({ SourceContextLayerOptIn: () => null }));
jest.mock("cloud/components/AgentsOptIn/useShowAgentsOptIn", () => ({ useShowAgentsOptIn: jest.fn() }));

jest.mock("core/api", () => ({
  useAgentsProvisioningStatus: jest.fn(),
  useAgentsSupportedSourceDefinitionIds: jest.fn(),
  useCreateSource: jest.fn(),
  useGetSourceDefinitionSpecificationAsync: jest.fn(),
  useSetFusionActorEnablement: jest.fn(),
  useSourceDefinitionList: jest.fn(),
  useUpdateSource: jest.fn(),
}));

jest.mock("core/services/analytics", () => ({
  PageTrackingCodes: { SOURCE_NEW: "source-new" },
  useTrackPage: jest.fn(),
}));
jest.mock("core/services/FormChangeTracker", () => ({
  useFormChangeTrackerService: () => ({ clearAllFormChanges: jest.fn() }),
}));
jest.mock("core/utils/app", () => ({ useIsCloudApp: jest.fn() }));
jest.mock("core/utils/rbac", () => ({
  ...jest.requireActual("core/utils/rbac"),
  useGeneratedIntent: jest.fn(),
}));

const sourceValues = {
  name: "Source",
  serviceType: "source-definition",
  connectionConfiguration: {},
};

interface SourceFormCallbacks {
  onSubmit: (values: typeof sourceValues) => Promise<void>;
  onDraftPromoted: (source: typeof mockSource, values: { serviceType: string }) => Promise<void>;
}

describe("CreateSourcePage context layer enablement", () => {
  const createSource = jest.fn();
  const setFusionActorEnablement = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
    createSource.mockResolvedValue(mockSource);
    setFusionActorEnablement.mockResolvedValue(undefined);
    jest
      .mocked(useCreateSource)
      .mockReturnValue({ mutateAsync: createSource } as unknown as ReturnType<typeof useCreateSource>);
    jest
      .mocked(useSetFusionActorEnablement)
      .mockReturnValue({ mutateAsync: setFusionActorEnablement } as unknown as ReturnType<
        typeof useSetFusionActorEnablement
      >);
    jest
      .mocked(useUpdateSource)
      .mockReturnValue({ mutateAsync: jest.fn() } as unknown as ReturnType<typeof useUpdateSource>);
    jest.mocked(useSourceDefinitionList).mockReturnValue({
      sourceDefinitions: [{ sourceDefinitionId: sourceValues.serviceType }],
    } as ReturnType<typeof useSourceDefinitionList>);
    jest.mocked(useGetSourceDefinitionSpecificationAsync).mockReturnValue({
      isLoading: false,
    } as ReturnType<typeof useGetSourceDefinitionSpecificationAsync>);
    jest
      .mocked(useAgentsProvisioningStatus)
      .mockReturnValue({ is_enrolled: true } as ReturnType<typeof useAgentsProvisioningStatus>);
    jest.mocked(useAgentsSupportedSourceDefinitionIds).mockReturnValue(new Set([sourceValues.serviceType]));
    jest.mocked(useIsCloudApp).mockReturnValue(true);
    jest.mocked(useShowAgentsOptIn).mockReturnValue(true);
    jest.mocked(useGeneratedIntent).mockReturnValue(true);
  });

  it.each([false, true])("sets indexing to %s when creating a source", async (enabled) => {
    mockExperiments({ "connector.agentAssistedSetup": true, "platform.fusion-semantic-search-ui": enabled });
    await render(<CreateSourcePage />);
    const { onSubmit } = mockSourceFormWithAgent.mock.calls[0][0] as SourceFormCallbacks;

    jest.useFakeTimers();
    try {
      await act(async () => {
        const submission = onSubmit(sourceValues);
        await jest.runAllTimersAsync();
        await submission;
      });
    } finally {
      jest.useRealTimers();
    }

    expect(setFusionActorEnablement).toHaveBeenCalledWith({
      actorId: mockSource.sourceId,
      actorKind: "source",
      workspaceId: mockSource.workspaceId,
      state: { enable_agent_access: true, enable_indexing: enabled },
    });
  });

  it.each([false, true])("sets indexing to %s when promoting a draft", async (enabled) => {
    mockExperiments({ "connector.agentAssistedSetup": true, "platform.fusion-semantic-search-ui": enabled });
    await render(<CreateSourcePage />);
    const { onDraftPromoted } = mockSourceFormWithAgent.mock.calls[0][0] as SourceFormCallbacks;

    await act(async () => onDraftPromoted(mockSource, { serviceType: sourceValues.serviceType }));

    expect(setFusionActorEnablement).toHaveBeenCalledWith({
      actorId: mockSource.sourceId,
      actorKind: "source",
      workspaceId: mockSource.workspaceId,
      state: { enable_agent_access: true, enable_indexing: enabled },
    });
  });
});
