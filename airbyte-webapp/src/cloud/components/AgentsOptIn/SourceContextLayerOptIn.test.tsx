import { fireEvent, render, screen } from "@testing-library/react";
import { useState } from "react";
import { IntlProvider } from "react-intl";

import { mockExperiments } from "test-utils/mockExperiments";

import { ConnectorIds } from "area/connector/utils/constants";
import { useAgentsProvisioningStatus, useAgentsSupportedSourceDefinitionIds } from "core/api";
import { useIsCloudApp } from "core/utils/app";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

import { SourceContextLayerOptIn } from "./SourceContextLayerOptIn";
import { useShowAgentsOptIn } from "./useShowAgentsOptIn";

jest.mock("core/api", () => ({
  useAgentsProvisioningStatus: jest.fn(),
  useAgentsSupportedSourceDefinitionIds: jest.fn(),
}));

jest.mock("core/utils/app", () => ({
  useIsCloudApp: jest.fn(),
}));

jest.mock("core/utils/rbac", () => ({
  ...jest.requireActual("core/utils/rbac"),
  useGeneratedIntent: jest.fn(),
}));

jest.mock("./useShowAgentsOptIn", () => ({
  useShowAgentsOptIn: jest.fn(),
}));

const mockUseAgentsProvisioningStatus = useAgentsProvisioningStatus as jest.MockedFunction<
  typeof useAgentsProvisioningStatus
>;
const mockUseAgentsSupportedSourceDefinitionIds = useAgentsSupportedSourceDefinitionIds as jest.MockedFunction<
  typeof useAgentsSupportedSourceDefinitionIds
>;
const mockUseIsCloudApp = useIsCloudApp as jest.MockedFunction<typeof useIsCloudApp>;
const mockUseShowAgentsOptIn = useShowAgentsOptIn as jest.MockedFunction<typeof useShowAgentsOptIn>;
const mockUseGeneratedIntent = useGeneratedIntent as jest.MockedFunction<typeof useGeneratedIntent>;

const messages = {
  "cloud.contextLayer.setup.agentAccess.title": "Agent access",
  "cloud.contextLayer.setup.agentAccess.description":
    "Agents can use the Airbyte MCP to read from and write to this {actorType, select, source {source} other {destination}} directly. No sync required.",
  "cloud.contextLayer.sourceOptIn.noPermission":
    "You need edit permission for this workspace's sources to change this.",
  "cloud.contextLayer.actor.notEnrolled":
    "An organization admin needs to enable the Context layer for this organization and workspace before agent access can be turned on.",
};

const initialValue = true;

const renderOptIn = (value: boolean = initialValue, onChange: (value: boolean) => void = jest.fn()) =>
  render(
    <IntlProvider locale="en" messages={messages}>
      <SourceContextLayerOptIn sourceDefinitionId={ConnectorIds.Sources.GitHub} value={value} onChange={onChange} />
    </IntlProvider>
  );

describe("SourceContextLayerOptIn", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockUseAgentsProvisioningStatus.mockReturnValue({
      is_enrolled: true,
      is_instance_admin: false,
      provisioning_state: "provisioned",
      organization_id: "org-id",
      organization_kind: "external_cloud",
      external_cloud_eligible: true,
      eligible_external_organization_id: null,
    });
    mockUseAgentsSupportedSourceDefinitionIds.mockReturnValue(new Set([ConnectorIds.Sources.GitHub]));
    mockUseIsCloudApp.mockReturnValue(true);
    mockUseShowAgentsOptIn.mockReturnValue(true);
    mockUseGeneratedIntent.mockReturnValue(true);
    mockExperiments({ "platform.fusion-semantic-search-ui": true });
  });

  it("renders a disabled unchecked toggle with an enrollment tooltip before enrollment", async () => {
    mockUseAgentsProvisioningStatus.mockReturnValue(null);
    const onChange = jest.fn();
    renderOptIn(initialValue, onChange);

    const toggles = screen.getAllByRole("checkbox");
    toggles.forEach((toggle) => {
      expect(toggle).toBeDisabled();
      expect(toggle).not.toBeChecked();
      fireEvent.click(toggle);
    });
    expect(onChange).not.toHaveBeenCalled();
    fireEvent.mouseOver(toggles[0]);
    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      "An organization admin needs to enable the Context layer for this organization and workspace before agent access can be turned on."
    );
  });

  it("renders nothing outside the cloud app or when the gate is disabled", () => {
    mockUseIsCloudApp.mockReturnValue(false);
    renderOptIn();
    expect(screen.queryByRole("checkbox")).not.toBeInTheDocument();

    mockUseIsCloudApp.mockReturnValue(true);
    mockUseShowAgentsOptIn.mockReturnValue(false);
    renderOptIn();
    expect(screen.queryByRole("checkbox")).not.toBeInTheDocument();
  });

  it("renders nothing when the source definition ID is unavailable", () => {
    render(
      <IntlProvider locale="en" messages={messages}>
        <SourceContextLayerOptIn value={initialValue} onChange={jest.fn()} />
      </IntlProvider>
    );
    expect(screen.queryByRole("checkbox")).not.toBeInTheDocument();
  });

  it.each([false, true])("renders only agent access when the semantic search flag is %s", (enabled) => {
    mockExperiments({ "platform.fusion-semantic-search-ui": enabled });
    renderOptIn();
    expect(screen.getAllByRole("checkbox")).toHaveLength(1);
    expect(screen.getByRole("checkbox", { name: "Agent access" })).toBeChecked();
    expect(screen.getByRole("group", { name: "Agent access" })).toContainElement(
      screen.getByRole("checkbox", { name: "Agent access" })
    );
    expect(screen.queryByText(/semantic search/i)).not.toBeInTheDocument();
    expect(
      screen.getByText(
        "Agents can use the Airbyte MCP to read from and write to this source directly. No sync required."
      )
    ).toBeInTheDocument();
    expect(mockUseGeneratedIntent).toHaveBeenCalledWith(Intent.CreateOrEditSource);
  });

  it("renders nothing for an unsupported source", () => {
    const { container } = render(
      <IntlProvider locale="en" messages={messages}>
        <SourceContextLayerOptIn sourceDefinitionId="not-supported" value={initialValue} onChange={jest.fn()} />
      </IntlProvider>
    );

    expect(container).toBeEmptyDOMElement();
  });

  it("renders nothing when no source definitions are supported", () => {
    mockUseAgentsSupportedSourceDefinitionIds.mockReturnValue(new Set());

    const { container } = renderOptIn();

    expect(container).toBeEmptyDOMElement();
  });

  it("updates agent access with a boolean when toggled off and on", () => {
    const onChange = jest.fn();
    const Wrapper = () => {
      const [value, setValue] = useState(initialValue);
      return (
        <SourceContextLayerOptIn
          sourceDefinitionId={ConnectorIds.Sources.GitHub}
          value={value}
          onChange={(nextValue) => {
            onChange(nextValue);
            setValue(nextValue);
          }}
        />
      );
    };

    render(
      <IntlProvider locale="en" messages={messages}>
        <Wrapper />
      </IntlProvider>
    );
    const agentAccess = screen.getByRole("checkbox", { name: "Agent access" });

    fireEvent.click(agentAccess);

    expect(onChange).toHaveBeenLastCalledWith(false);
    expect(agentAccess).not.toBeChecked();
    fireEvent.click(agentAccess);
    expect(onChange).toHaveBeenLastCalledWith(true);
    expect(agentAccess).toBeChecked();
  });

  it("disables the toggle and does not call onChange without permission", async () => {
    const onChange = jest.fn();
    mockUseGeneratedIntent.mockReturnValue(false);
    renderOptIn(initialValue, onChange);

    const toggles = screen.getAllByRole("checkbox");
    toggles.forEach((toggle) => expect(toggle).toBeDisabled());
    toggles.forEach((toggle) => expect(toggle).toBeChecked());

    toggles.forEach((toggle) => fireEvent.click(toggle));

    expect(onChange).not.toHaveBeenCalled();
    fireEvent.mouseOver(toggles[0]);
    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      "You need edit permission for this workspace's sources to change this."
    );
  });
});
