import { fireEvent, render, screen } from "@testing-library/react";
import { IntlProvider } from "react-intl";

import { useAgentsProvisioningStatus, useAgentsSupportedDestinationDefinitionIds } from "core/api";
import { useIsCloudApp } from "core/utils/app";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

import { DestinationContextLayerOptIn } from "./DestinationContextLayerOptIn";
import { useShowAgentsOptIn } from "./useShowAgentsOptIn";

jest.mock("core/api", () => ({
  useAgentsProvisioningStatus: jest.fn(),
  useAgentsSupportedDestinationDefinitionIds: jest.fn(),
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
const mockUseAgentsSupportedDestinationDefinitionIds =
  useAgentsSupportedDestinationDefinitionIds as jest.MockedFunction<typeof useAgentsSupportedDestinationDefinitionIds>;
const mockUseIsCloudApp = useIsCloudApp as jest.MockedFunction<typeof useIsCloudApp>;
const mockUseShowAgentsOptIn = useShowAgentsOptIn as jest.MockedFunction<typeof useShowAgentsOptIn>;
const mockUseGeneratedIntent = useGeneratedIntent as jest.MockedFunction<typeof useGeneratedIntent>;

const messages = {
  "cloud.contextLayer.actor.notSupported": "This connector is not yet supported by the context layer.",
  "cloud.contextLayer.actor.notEnrolled":
    "An organization admin needs to enable the Context layer for this organization and workspace before agent access can be turned on.",
  "cloud.contextLayer.agentAccess.title": "Agent Access",
  "cloud.contextLayer.agentAccess.description":
    "Allow AI agents with context layer access to query this {actorType, select, source {source} other {destination}} directly. This does not affect data replication.",
  "cloud.contextLayer.destinationOptIn.noPermission":
    "You need edit permission for this workspace's destinations to change this.",
};

const renderOptIn = (
  destinationDefinitionId = "destination-definition-id",
  value = true,
  onChange: (value: boolean) => void = jest.fn()
) =>
  render(
    <IntlProvider locale="en" messages={messages}>
      <DestinationContextLayerOptIn
        destinationDefinitionId={destinationDefinitionId}
        value={value}
        onChange={onChange}
      />
    </IntlProvider>
  );

describe("DestinationContextLayerOptIn", () => {
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
    mockUseAgentsSupportedDestinationDefinitionIds.mockReturnValue(new Set(["destination-definition-id"]));
    mockUseIsCloudApp.mockReturnValue(true);
    mockUseShowAgentsOptIn.mockReturnValue(true);
    mockUseGeneratedIntent.mockReturnValue(true);
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

  it("renders nothing when the destination definition id is unavailable", () => {
    render(
      <IntlProvider locale="en" messages={messages}>
        <DestinationContextLayerOptIn value onChange={jest.fn()} />
      </IntlProvider>
    );

    expect(screen.queryByRole("checkbox")).not.toBeInTheDocument();
  });

  it("renders a disabled unchecked toggle with an enrollment tooltip before enrollment", async () => {
    mockUseAgentsProvisioningStatus.mockReturnValue(null);
    const onChange = jest.fn();
    renderOptIn("destination-definition-id", true, onChange);

    const toggle = screen.getByRole("checkbox");
    expect(toggle).toBeDisabled();
    expect(toggle).not.toBeChecked();
    fireEvent.click(toggle);
    expect(onChange).not.toHaveBeenCalled();
    fireEvent.mouseOver(toggle);
    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      "An organization admin needs to enable the Context layer for this organization and workspace before agent access can be turned on."
    );
  });

  it("renders a disabled unchecked toggle and unsupported footnote for an unsupported destination", async () => {
    renderOptIn("unsupported-definition-id");

    const toggle = screen.getByRole("checkbox");
    expect(toggle).toBeDisabled();
    expect(toggle).not.toBeChecked();
    expect(screen.getByText("This connector is not yet supported by the context layer.")).toBeInTheDocument();
    fireEvent.mouseOver(toggle);
    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      "This connector is not yet supported by the context layer."
    );
  });

  it("disables the toggle and shows a permission tooltip when the destination cannot be edited", async () => {
    const onChange = jest.fn();
    mockUseGeneratedIntent.mockReturnValue(false);
    renderOptIn("destination-definition-id", true, onChange);

    const toggle = screen.getByRole("checkbox");
    expect(toggle).toBeDisabled();
    expect(toggle).toBeChecked();
    fireEvent.click(toggle);
    expect(onChange).not.toHaveBeenCalled();
    fireEvent.mouseOver(toggle);
    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      "You need edit permission for this workspace's destinations to change this."
    );
  });

  it("renders an enrolled supported destination and calls onChange", () => {
    const onChange = jest.fn();
    renderOptIn("destination-definition-id", true, onChange);

    const toggle = screen.getByRole("checkbox");
    expect(toggle).toBeEnabled();
    expect(toggle).toBeChecked();
    expect(mockUseGeneratedIntent).toHaveBeenCalledWith(Intent.CreateOrEditDestination);

    fireEvent.click(toggle);

    expect(onChange).toHaveBeenCalledWith(false);
  });
});
