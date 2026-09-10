import { fireEvent, render, screen } from "@testing-library/react";
import { useState } from "react";
import { IntlProvider } from "react-intl";

import { useAgentsProvisioningStatus, useAgentsSupportedSourceDefinitions } from "core/api";
import { useIsCloudApp } from "core/utils/app";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

import { SourceContextLayerOptIn, SourceContextLayerOptInValue } from "./SourceContextLayerOptIn";
import { useShowAgentsOptIn } from "./useShowAgentsOptIn";

jest.mock("core/api", () => ({
  useAgentsProvisioningStatus: jest.fn(),
  useAgentsSupportedSourceDefinitions: jest.fn(),
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
const mockUseAgentsSupportedSourceDefinitions = useAgentsSupportedSourceDefinitions as jest.MockedFunction<
  typeof useAgentsSupportedSourceDefinitions
>;
const mockUseIsCloudApp = useIsCloudApp as jest.MockedFunction<typeof useIsCloudApp>;
const mockUseShowAgentsOptIn = useShowAgentsOptIn as jest.MockedFunction<typeof useShowAgentsOptIn>;
const mockUseGeneratedIntent = useGeneratedIntent as jest.MockedFunction<typeof useGeneratedIntent>;

const messages = {
  "cloud.contextLayer.actor.notSupported": "This connector is not yet supported by the context layer.",
  "cloud.contextLayer.sourceOptIn.title": "Make this source available to the context layer",
  "cloud.contextLayer.sourceOptIn.description":
    "When enabled, AI agents with context layer access can query this source directly. This does not affect data replication.",
  "cloud.contextLayer.sourceOptIn.semanticSearch.title": "Also index this source for semantic search",
  "cloud.contextLayer.sourceOptIn.semanticSearch.description":
    "Grants Airbyte permission to store and index a copy of this source's data in Airbyte data centers so agents can search it. This is in addition to your data replication jobs.",
  "cloud.contextLayer.sourceOptIn.noPermission":
    "You need edit permission for this workspace's sources to change this.",
  "cloud.contextLayer.sourceOptIn.notEnrolled":
    "An organization admin needs to enable the Context layer for this organization and workspace before this source can be made available.",
};

const initialValue: SourceContextLayerOptInValue = { agentAccess: true, semanticSearch: true };

const renderOptIn = (
  value: SourceContextLayerOptInValue = initialValue,
  onChange: (value: SourceContextLayerOptInValue) => void = jest.fn()
) =>
  render(
    <IntlProvider locale="en" messages={messages}>
      <SourceContextLayerOptIn sourceDefinitionName="GitHub" value={value} onChange={onChange} />
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
    mockUseAgentsSupportedSourceDefinitions.mockReturnValue(new Set(["GitHub"]));
    mockUseIsCloudApp.mockReturnValue(true);
    mockUseShowAgentsOptIn.mockReturnValue(true);
    mockUseGeneratedIntent.mockReturnValue(true);
  });

  it("renders disabled unchecked toggles with an enrollment tooltip before enrollment", async () => {
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
      "An organization admin needs to enable the Context layer for this organization and workspace before this source can be made available."
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

  it("renders nothing when the source definition name is unavailable", () => {
    render(
      <IntlProvider locale="en" messages={messages}>
        <SourceContextLayerOptIn value={initialValue} onChange={jest.fn()} />
      </IntlProvider>
    );
    expect(screen.queryByRole("checkbox")).not.toBeInTheDocument();
  });

  it("renders both toggles checked for a supported source", () => {
    renderOptIn();
    expect(screen.getAllByRole("checkbox")).toHaveLength(2);
    expect(screen.getAllByRole("checkbox").every((toggle) => (toggle as HTMLInputElement).checked)).toBe(true);
    expect(mockUseGeneratedIntent).toHaveBeenCalledWith(Intent.CreateOrEditSource);
  });

  it("renders both toggles disabled and shows an unsupported tooltip for an unsupported source", async () => {
    render(
      <IntlProvider locale="en" messages={messages}>
        <SourceContextLayerOptIn sourceDefinitionName="Not Supported" value={initialValue} onChange={jest.fn()} />
      </IntlProvider>
    );

    screen.getAllByRole("checkbox").forEach((toggle) => {
      expect(toggle).toBeDisabled();
      expect(toggle).not.toBeChecked();
    });
    fireEvent.mouseOver(screen.getAllByRole("checkbox")[0]);
    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      "This connector is not yet supported by the context layer."
    );
  });

  it("disables and unchecks semantic search when agent access is turned off", () => {
    const onChange = jest.fn();
    const Wrapper = () => {
      const [value, setValue] = useState(initialValue);
      return (
        <SourceContextLayerOptIn
          sourceDefinitionName="GitHub"
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
    const [agentAccess, semanticSearch] = screen.getAllByRole("checkbox");

    fireEvent.click(agentAccess);

    expect(onChange).toHaveBeenCalledWith({ agentAccess: false, semanticSearch: true });
    expect(agentAccess).not.toBeChecked();
    expect(semanticSearch).toBeDisabled();
    expect(semanticSearch).not.toBeChecked();
  });

  it("calls onChange when semantic search is toggled", () => {
    const onChange = jest.fn();
    renderOptIn(initialValue, onChange);

    fireEvent.click(screen.getAllByRole("checkbox")[1]);

    expect(onChange).toHaveBeenCalledWith({ agentAccess: true, semanticSearch: false });
  });

  it("disables both toggles and does not call onChange without permission", async () => {
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
