import { act, render, screen } from "@testing-library/react";
import { ReactNode } from "react";

import { ActorType, SourceRead } from "core/api/types/AirbyteClient";

import { ConnectorSetupAgentConfig, ConnectorSetupViews } from "./ConnectorSetupViews";
import { useConnectorSetupAgentState } from "./hooks/useConnectorSetupAgentState";
import { useConnectorSetupAgentAnalytics } from "./useAnalyticsTrackFunctions";

interface ToolsProps {
  onSubmitStep: (values: { name: string; serviceType: string; connectionConfiguration: object }) => Promise<void>;
}

const mockToolsProps = jest.fn((_props: ToolsProps) => undefined);

jest.mock("./ConnectorSetupAgent", () => ({
  ConnectorSetupAgent: () => <div data-testid="agent-chat" />,
}));
jest.mock("./ConnectorSetupAgentTools", () => ({
  ConnectorSetupAgentTools: (props: ToolsProps) => {
    mockToolsProps(props);
    return <div data-testid="agent-tools" />;
  },
}));
jest.mock("./hooks/useConnectorSetupAgentState", () => ({ useConnectorSetupAgentState: jest.fn() }));
jest.mock("./useAnalyticsTrackFunctions", () => ({ useConnectorSetupAgentAnalytics: jest.fn() }));

const trackAgentStarted = jest.fn();
const trackConfigurationSubmitted = jest.fn();

const renderSetupForm = (formAutomation?: ReactNode) => <div data-testid="setup-form">{formAutomation}</div>;

const agentConfig = (overrides: Partial<ConnectorSetupAgentConfig<SourceRead>> = {}) => ({
  actorType: ActorType.source,
  actorDefinitionId: "source-definition-id",
  connectionSpecification: {},
  isAvailable: true,
  onSubmit: jest.fn().mockResolvedValue(undefined),
  ...overrides,
});

const submitValues = { name: "Source", serviceType: "source-definition-id", connectionConfiguration: {} };

const submitThroughAgentTools = () => mockToolsProps.mock.calls[mockToolsProps.mock.calls.length - 1][0].onSubmitStep;

describe("ConnectorSetupViews", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    jest.mocked(useConnectorSetupAgentAnalytics).mockReturnValue({
      trackAgentStarted,
      trackMessageSent: jest.fn(),
      trackConfigurationChecked: jest.fn(),
      trackConfigurationSubmitted,
    });
    jest.mocked(useConnectorSetupAgentState).mockReturnValue({
      handleClientToolsReady: jest.fn(),
      secretInputState: {
        isSecretInputActive: false,
        secretFieldPath: [],
        secretFieldName: undefined,
        isMultiline: false,
        submitSecret: jest.fn(),
        dismissSecret: jest.fn(),
      },
      handleSecretInputStateChange: jest.fn(),
      oauthState: { isOAuthPendingUserAction: false, startOAuth: jest.fn(), cancelOAuth: jest.fn() },
      handleOAuthStateChange: jest.fn(),
      handleFormValuesReady: jest.fn(),
      touchedSecretFieldsRef: { current: new Set() },
      addTouchedSecretField: jest.fn(),
      messages: [],
      sendMessage: jest.fn(),
      isLoading: false,
      error: null,
      stopGenerating: jest.fn(),
      isStreaming: false,
    });
  });

  it("keeps the setup form mounted while the agent view shows", () => {
    render(<ConnectorSetupViews agent={agentConfig()} isAgentView renderSetupForm={renderSetupForm} />);

    expect(screen.getByTestId("agent-chat")).toBeInTheDocument();
    expect(screen.getByTestId("setup-form")).toBeInTheDocument();
    expect(screen.getByTestId("setup-form").parentElement).toHaveClass("hidden");
    expect(trackAgentStarted).toHaveBeenCalledWith("source", "source-definition-id");
  });

  it("renders the agent tools inside the setup form when the agent is available", () => {
    render(<ConnectorSetupViews agent={agentConfig()} isAgentView={false} renderSetupForm={renderSetupForm} />);

    expect(screen.getByTestId("setup-form")).toContainElement(screen.getByTestId("agent-tools"));
  });

  it("shows only the form while the agent is not available", () => {
    render(
      <ConnectorSetupViews
        agent={agentConfig({ isAvailable: false, connectionSpecification: undefined })}
        isAgentView
        renderSetupForm={renderSetupForm}
      />
    );

    expect(screen.getByTestId("setup-form").parentElement).toHaveClass("visible");
    expect(screen.queryByTestId("agent-chat")).not.toBeInTheDocument();
    expect(screen.queryByTestId("agent-tools")).not.toBeInTheDocument();
    expect(useConnectorSetupAgentState).toHaveBeenLastCalledWith(expect.objectContaining({ isAgentView: false }));
    expect(trackAgentStarted).not.toHaveBeenCalled();
  });

  it("runs no agent code without an agent", () => {
    render(<ConnectorSetupViews isAgentView renderSetupForm={renderSetupForm} />);

    expect(screen.getByTestId("setup-form")).toBeInTheDocument();
    expect(screen.queryByTestId("agent-tools")).not.toBeInTheDocument();
    expect(useConnectorSetupAgentState).not.toHaveBeenCalled();
    expect(useConnectorSetupAgentAnalytics).not.toHaveBeenCalled();
  });

  it("tags agent submissions with the agent setup flow", async () => {
    const agent = agentConfig();
    render(<ConnectorSetupViews agent={agent} isAgentView renderSetupForm={renderSetupForm} />);

    await act(() => submitThroughAgentTools()(submitValues));

    expect(agent.onSubmit).toHaveBeenCalledWith({ ...submitValues, setupFlow: "agent" });
    expect(trackConfigurationSubmitted).toHaveBeenCalledWith("source", "source-definition-id", true);
  });

  it("tracks and rethrows a failed agent submission", async () => {
    const agent = agentConfig({ onSubmit: jest.fn().mockRejectedValue(new Error("create failed")) });
    render(<ConnectorSetupViews agent={agent} isAgentView renderSetupForm={renderSetupForm} />);

    await expect(submitThroughAgentTools()(submitValues)).rejects.toThrow("create failed");
    expect(trackConfigurationSubmitted).toHaveBeenCalledWith("source", "source-definition-id", false);
  });
});
