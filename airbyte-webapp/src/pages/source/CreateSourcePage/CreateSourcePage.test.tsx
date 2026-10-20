import { act, screen } from "@testing-library/react";
import { PropsWithChildren, ReactElement, ReactNode } from "react";
import { useParams } from "react-router-dom";

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
import { useTrackPage } from "core/services/analytics";
import { useNotificationService } from "core/services/Notification";
import { useIsCloudApp } from "core/utils/app";
import { useGeneratedIntent } from "core/utils/rbac";

import { CreateSourcePage } from "./CreateSourcePage";

const mockSourceForm = jest.fn((_props: unknown) => null);
const mockFormAutomation = <span data-testid="form-automation" />;
const mockConnectorSetupViews = jest.fn(
  ({
    agent,
    renderSetupForm,
  }: {
    agent?: { isAvailable: boolean };
    renderSetupForm: (formAutomation?: ReactNode) => ReactNode;
  }) => <>{renderSetupForm(agent?.isAvailable ? mockFormAutomation : undefined)}</>
);

jest.mock("./SourceForm", () => ({
  SourceForm: (props: unknown) => mockSourceForm(props),
}));
jest.mock("area/connector/components/agents/ConnectorSetupViews", () => ({
  ConnectorSetupViews: (props: {
    agent?: { isAvailable: boolean };
    renderSetupForm: (formAutomation?: ReactNode) => ReactNode;
  }) => mockConnectorSetupViews(props),
}));
jest.mock("area/connector/components/ConnectorDocumentationLayout/ConnectorDocumentationWrapper", () => ({
  ConnectorDocumentationWrapper: ({ children }: PropsWithChildren) => <>{children}</>,
}));
jest.mock("area/organization/components/CloudInviteUsersHint", () => ({ CloudInviteUsersHint: () => null }));

jest.mock("react-router-dom", () => ({
  ...jest.requireActual("react-router-dom"),
  useParams: jest.fn(),
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
}

interface SourceFormProps extends SourceFormCallbacks {
  formAutomation?: ReactNode;
  contextLayerOptIn: ReactElement<{ value: boolean; onChange: (value: boolean) => void }>;
}

interface SetupViewsProps {
  agent?: SourceFormCallbacks & { isAvailable: boolean };
}

const latestForm = () => mockSourceForm.mock.calls[mockSourceForm.mock.calls.length - 1][0] as SourceFormProps;
const latestViews = () =>
  mockConnectorSetupViews.mock.calls[mockConnectorSetupViews.mock.calls.length - 1][0] as SetupViewsProps;

const completeSource = async (flow: "create" | "promote", setupFlow?: "agent") => {
  // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
  const form = setupFlow === "agent" ? latestViews().agent! : latestForm();
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

const createSource = jest.fn();
const setFusionActorEnablement = jest.fn();
const registerNotification = jest.fn();

beforeEach(() => {
  jest.clearAllMocks();
  jest.mocked(useParams).mockReturnValue({ workspaceId: "workspace-id", sourceDefinitionId: sourceValues.serviceType });
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

describe("CreateSourcePage context layer enablement", () => {
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

describe("CreateSourcePage agent availability", () => {
  const specification = { sourceDefinitionId: sourceValues.serviceType, connectionSpecification: {} };

  const mockSpecification = (spec: { isLoading: boolean; data?: object; error?: Error }) =>
    jest
      .mocked(useGetSourceDefinitionSpecificationAsync)
      .mockReturnValue(spec as unknown as ReturnType<typeof useGetSourceDefinitionSpecificationAsync>);

  it("shows the Agent/Form toggle when the agent is available", async () => {
    mockExperiments({ "connector.agentAssistedSetup": true });
    mockSpecification({ isLoading: false, data: specification });
    await render(<CreateSourcePage />);

    expect(screen.getByText("Agent")).toBeInTheDocument();
    expect(latestViews().agent?.isAvailable).toBe(true);
    expect(latestForm().formAutomation).toBe(mockFormAutomation);
    expect(useTrackPage).toHaveBeenLastCalledWith("source-new", { agent_toggle_available: true });
  });

  it("hides the toggle and passes no agent when the experiment is off", async () => {
    mockExperiments({ "connector.agentAssistedSetup": false });
    mockSpecification({ isLoading: false, data: specification });
    await render(<CreateSourcePage />);

    expect(screen.queryByText("Agent")).not.toBeInTheDocument();
    expect(latestViews().agent).toBeUndefined();
    expect(latestForm().formAutomation).toBeUndefined();
    expect(useTrackPage).toHaveBeenLastCalledWith("source-new", { agent_toggle_available: false });
  });

  it("hides the toggle for a custom connector", async () => {
    mockExperiments({ "connector.agentAssistedSetup": true });
    mockSpecification({ isLoading: false, data: specification });
    jest.mocked(useSourceDefinitionList).mockReturnValue({
      sourceDefinitions: [{ sourceDefinitionId: sourceValues.serviceType, custom: true }],
    } as ReturnType<typeof useSourceDefinitionList>);
    await render(<CreateSourcePage />);

    expect(screen.queryByText("Agent")).not.toBeInTheDocument();
    expect(latestViews().agent?.isAvailable).toBe(false);
  });

  it.each([
    ["while the spec loads", { isLoading: true }],
    ["when the spec fails to load", { isLoading: false, error: new Error("spec failed") }],
  ])("hides the toggle %s", async (_, spec) => {
    mockExperiments({ "connector.agentAssistedSetup": true });
    mockSpecification(spec);
    await render(<CreateSourcePage />);

    expect(screen.queryByText("Agent")).not.toBeInTheDocument();
    expect(latestViews().agent?.isAvailable).toBe(false);
    expect(useTrackPage).toHaveBeenLastCalledWith("source-new", { agent_toggle_available: false });
  });

  it("sends form and agent submissions through the same create handler", async () => {
    mockExperiments({ "connector.agentAssistedSetup": true });
    mockSpecification({ isLoading: false, data: specification });
    await render(<CreateSourcePage />);
    expect(latestViews().agent?.onSubmit).toBe(latestForm().onSubmit);

    await completeSource("create");
    expect(createSource).toHaveBeenLastCalledWith(
      expect.objectContaining({ values: { ...sourceValues, setupFlow: undefined } })
    );

    await completeSource("create", "agent");
    expect(createSource).toHaveBeenLastCalledWith(
      expect.objectContaining({ values: { ...sourceValues, setupFlow: "agent" } })
    );
  });
});
