import { act } from "@testing-library/react";
import { ReactElement } from "react";

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
import { useNotificationService } from "core/services/Notification";
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
jest.mock("core/services/Notification", () => ({
  ...jest.requireActual("core/services/Notification"),
  useNotificationService: jest.fn(),
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
  onSubmit: (values: typeof sourceValues & { setupFlow?: "agent" }) => Promise<void>;
  onDraftPromoted: (source: typeof mockSource, values: { serviceType: string; setupFlow?: "agent" }) => Promise<void>;
  contextLayerOptIn: ReactElement<{ value: boolean; onChange: (value: boolean) => void }>;
}

const latestForm = () =>
  mockSourceFormWithAgent.mock.calls[mockSourceFormWithAgent.mock.calls.length - 1][0] as SourceFormCallbacks;

const completeSource = async (flow: "create" | "promote", setupFlow?: "agent") => {
  const form = latestForm();
  if (flow === "promote") {
    await act(async () => form.onDraftPromoted(mockSource, { serviceType: sourceValues.serviceType, setupFlow }));
    return;
  }

  jest.useFakeTimers();
  try {
    await act(async () => {
      const submission = form.onSubmit({ ...sourceValues, setupFlow });
      await jest.runAllTimersAsync();
      await submission;
    });
  } finally {
    jest.useRealTimers();
  }
};

describe("CreateSourcePage context layer enablement", () => {
  const createSource = jest.fn();
  const setFusionActorEnablement = jest.fn();
  const registerNotification = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
    jest.mocked(useNotificationService).mockReturnValue({
      registerNotification,
      unregisterAllNotifications: jest.fn(),
      unregisterNotificationById: jest.fn(),
    });
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

  it.each([
    ["create", false, false],
    ["create", false, true],
    ["create", true, false],
    ["create", true, true],
    ["promote", false, false],
    ["promote", false, true],
    ["promote", true, false],
    ["promote", true, true],
  ] as const)("%s sends only access=%s with semantic search flag=%s", async (flow, enabled, semanticSearchFlag) => {
    mockExperiments({ "connector.agentAssistedSetup": true, "platform.fusion-semantic-search-ui": semanticSearchFlag });
    await render(<CreateSourcePage />);
    expect(latestForm().contextLayerOptIn.props.value).toBe(true);
    act(() => latestForm().contextLayerOptIn.props.onChange(enabled));
    expect(latestForm().contextLayerOptIn.props.value).toBe(enabled);

    await completeSource(flow);

    expect(setFusionActorEnablement).toHaveBeenCalledWith({
      actorId: mockSource.sourceId,
      actorKind: "source",
      workspaceId: mockSource.workspaceId,
      enabled,
    });
  });

  it.each(["create", "promote"] as const)("does not enable access for agent-assisted %s", async (flow) => {
    mockExperiments({ "connector.agentAssistedSetup": true, "platform.fusion-semantic-search-ui": true });
    await render(<CreateSourcePage />);

    await completeSource(flow, "agent");

    expect(setFusionActorEnablement).not.toHaveBeenCalled();
  });

  it.each(["create", "promote"] as const)("does not enable access without edit permission during %s", async (flow) => {
    jest.mocked(useGeneratedIntent).mockReturnValue(false);
    mockExperiments({ "connector.agentAssistedSetup": true, "platform.fusion-semantic-search-ui": true });
    await render(<CreateSourcePage />);
    expect(latestForm().contextLayerOptIn.props.value).toBe(false);

    await completeSource(flow);

    expect(setFusionActorEnablement).not.toHaveBeenCalled();
  });

  it.each(["create", "promote"] as const)("keeps a completed %s when access saving fails", async (flow) => {
    setFusionActorEnablement.mockRejectedValue(new Error("Access update failed"));
    mockExperiments({ "connector.agentAssistedSetup": true, "platform.fusion-semantic-search-ui": true });
    await render(<CreateSourcePage />);

    await expect(completeSource(flow)).resolves.toBeUndefined();
    expect(setFusionActorEnablement).toHaveBeenCalledWith({
      actorId: mockSource.sourceId,
      actorKind: "source",
      workspaceId: mockSource.workspaceId,
      enabled: true,
    });
    expect(registerNotification).toHaveBeenCalledWith({
      id: "cloud.contextLayer.sourceOptIn.syncFailed",
      text: "Source created, but its agent access setting could not be saved. You can change it in Source settings.",
      type: "error",
    });
  });
});
