import { screen } from "@testing-library/react";
import { PropsWithChildren, ReactNode } from "react";
import { useParams } from "react-router-dom";

import { render } from "test-utils";
import { mockExperiments } from "test-utils/mockExperiments";

import { useShowAgentsOptIn } from "cloud/components/AgentsOptIn/useShowAgentsOptIn";
import {
  useAgentsProvisioningStatus,
  useAgentsSupportedDestinationDefinitionIds,
  useCreateDestination,
  useDestinationDefinitionList,
  useGetDestinationDefinitionSpecificationAsync,
  useSetFusionActorEnablement,
  useUpdateDestination,
} from "core/api";
import { useNotificationService } from "core/services/Notification";
import { useIsCloudApp } from "core/utils/app";
import { useGeneratedIntent } from "core/utils/rbac";

import { CreateDestinationPage } from "./CreateDestinationPage";

const mockDestinationForm = jest.fn((_props: unknown) => null);
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

jest.mock("react-router-dom", () => ({
  ...jest.requireActual("react-router-dom"),
  useParams: jest.fn(),
}));
jest.mock("area/connector/components/destination/DestinationForm", () => ({
  DestinationForm: (props: unknown) => mockDestinationForm(props),
}));
jest.mock("area/connector/components/agents/ConnectorSetupViews", () => ({
  ConnectorSetupViews: (props: {
    agent?: { isAvailable: boolean };
    renderSetupForm: (formAutomation?: ReactNode) => ReactNode;
  }) => mockConnectorSetupViews(props),
}));
jest.mock("area/connector/components/ConnectorDocumentationLayout", () => ({
  ConnectorDocumentationWrapper: ({ children }: PropsWithChildren) => <>{children}</>,
}));
jest.mock("area/organization/components/CloudInviteUsersHint", () => ({ CloudInviteUsersHint: () => null }));
jest.mock("components/ui/HeadTitle", () => ({ HeadTitle: () => null }));
jest.mock("cloud/components/AgentsOptIn", () => ({ DestinationContextLayerOptIn: () => null }));
jest.mock("cloud/components/AgentsOptIn/useShowAgentsOptIn", () => ({ useShowAgentsOptIn: jest.fn() }));

jest.mock("core/api", () => ({
  useAgentsProvisioningStatus: jest.fn(),
  useAgentsSupportedDestinationDefinitionIds: jest.fn(),
  useCreateDestination: jest.fn(),
  useDestinationDefinitionList: jest.fn(),
  useGetDestinationDefinitionSpecificationAsync: jest.fn(),
  useSetFusionActorEnablement: jest.fn(),
  useUpdateDestination: jest.fn(),
}));

jest.mock("core/services/analytics", () => ({
  PageTrackingCodes: { DESTINATION_NEW: "destination-new" },
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

const destinationDefinitionId = "destination-definition";
const specification = { destinationDefinitionId, connectionSpecification: {} };

const latestForm = () =>
  mockDestinationForm.mock.calls[mockDestinationForm.mock.calls.length - 1][0] as { formAutomation?: ReactNode };
const latestViews = () =>
  mockConnectorSetupViews.mock.calls[mockConnectorSetupViews.mock.calls.length - 1][0] as {
    agent?: { isAvailable: boolean };
  };

const mockSpecification = (spec: { isLoading: boolean; data?: object; error?: Error }) =>
  jest
    .mocked(useGetDestinationDefinitionSpecificationAsync)
    .mockReturnValue(spec as unknown as ReturnType<typeof useGetDestinationDefinitionSpecificationAsync>);

const mockDestinationDefinitions = (custom: boolean) =>
  jest.mocked(useDestinationDefinitionList).mockReturnValue({
    destinationDefinitions: [{ destinationDefinitionId, custom }],
  } as ReturnType<typeof useDestinationDefinitionList>);

describe("CreateDestinationPage agent availability", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    jest.mocked(useParams).mockReturnValue({ workspaceId: "workspace-id", destinationDefinitionId });
    jest.mocked(useNotificationService).mockReturnValue({
      registerNotification: jest.fn(),
      unregisterAllNotifications: jest.fn(),
      unregisterNotificationById: jest.fn(),
    });
    jest
      .mocked(useCreateDestination)
      .mockReturnValue({ mutateAsync: jest.fn() } as unknown as ReturnType<typeof useCreateDestination>);
    jest
      .mocked(useUpdateDestination)
      .mockReturnValue({ mutateAsync: jest.fn() } as unknown as ReturnType<typeof useUpdateDestination>);
    jest
      .mocked(useSetFusionActorEnablement)
      .mockReturnValue({ mutateAsync: jest.fn() } as unknown as ReturnType<typeof useSetFusionActorEnablement>);
    jest
      .mocked(useAgentsProvisioningStatus)
      .mockReturnValue({ is_enrolled: true } as ReturnType<typeof useAgentsProvisioningStatus>);
    jest.mocked(useAgentsSupportedDestinationDefinitionIds).mockReturnValue(new Set([destinationDefinitionId]));
    jest.mocked(useIsCloudApp).mockReturnValue(true);
    jest.mocked(useShowAgentsOptIn).mockReturnValue(true);
    jest.mocked(useGeneratedIntent).mockReturnValue(true);
    mockDestinationDefinitions(false);
    mockSpecification({ isLoading: false, data: specification });
  });

  it("shows the Agent/Form toggle when the agent is available", async () => {
    mockExperiments({ "connector.agentAssistedSetup": true });
    await render(<CreateDestinationPage />);

    expect(screen.getByText("Agent")).toBeInTheDocument();
    expect(latestViews().agent?.isAvailable).toBe(true);
    expect(latestForm().formAutomation).toBe(mockFormAutomation);
  });

  it("hides the toggle and passes no agent when the experiment is off", async () => {
    mockExperiments({ "connector.agentAssistedSetup": false });
    await render(<CreateDestinationPage />);

    expect(screen.queryByText("Agent")).not.toBeInTheDocument();
    expect(latestViews().agent).toBeUndefined();
    expect(latestForm().formAutomation).toBeUndefined();
  });

  it("hides the toggle for a custom connector", async () => {
    mockExperiments({ "connector.agentAssistedSetup": true });
    mockDestinationDefinitions(true);
    await render(<CreateDestinationPage />);

    expect(screen.queryByText("Agent")).not.toBeInTheDocument();
    expect(latestViews().agent?.isAvailable).toBe(false);
  });

  it.each([
    ["while the spec loads", { isLoading: true }],
    ["when the spec fails to load", { isLoading: false, error: new Error("spec failed") }],
  ])("hides the toggle %s", async (_, spec) => {
    mockExperiments({ "connector.agentAssistedSetup": true });
    mockSpecification(spec);
    await render(<CreateDestinationPage />);

    expect(screen.queryByText("Agent")).not.toBeInTheDocument();
    expect(latestViews().agent?.isAvailable).toBe(false);
  });
});
