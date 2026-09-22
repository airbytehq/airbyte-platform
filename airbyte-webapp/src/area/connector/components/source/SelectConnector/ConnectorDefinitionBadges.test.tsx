import { render, screen } from "@testing-library/react";
import { IntlProvider } from "react-intl";

import { useIsAdpOrganization } from "area/organization/utils";
import { useAgentsSupportedDestinationDefinitionIds, useAgentsSupportedSourceDefinitionIds } from "core/api";
import { ConnectorDefinitionOrEnterpriseStub } from "core/domain/connector";
import { useExperiment } from "core/services/Experiment";
import { useIsCloudApp } from "core/utils/app";

import { ConnectorDefinitionBadges } from "./ConnectorDefinitionBadges";

jest.mock("area/organization/utils", () => ({
  useIsAdpOrganization: jest.fn(),
}));

jest.mock("core/api", () => ({
  useAgentsSupportedDestinationDefinitionIds: jest.fn(),
  useAgentsSupportedSourceDefinitionIds: jest.fn(),
}));

jest.mock("core/services/Experiment", () => ({
  useExperiment: jest.fn(),
}));

jest.mock("core/utils/app", () => ({
  useIsCloudApp: jest.fn(),
}));

const mockUseAgentsSupportedDestinationDefinitionIds =
  useAgentsSupportedDestinationDefinitionIds as jest.MockedFunction<typeof useAgentsSupportedDestinationDefinitionIds>;
const mockUseAgentsSupportedSourceDefinitionIds = useAgentsSupportedSourceDefinitionIds as jest.MockedFunction<
  typeof useAgentsSupportedSourceDefinitionIds
>;
const mockUseExperiment = useExperiment as jest.MockedFunction<typeof useExperiment>;
const mockUseIsAdpOrganization = useIsAdpOrganization as jest.MockedFunction<typeof useIsAdpOrganization>;
const mockUseIsCloudApp = useIsCloudApp as jest.MockedFunction<typeof useIsCloudApp>;

const messages = {
  "connector.badge.agent": "Agent",
  "connector.badge.dataReplication": "Data replication",
};

const supportedSourceDefinition = {
  sourceDefinitionId: "supported-source-id",
  name: "Supported source",
} as ConnectorDefinitionOrEnterpriseStub;
const sameNameUnsupportedSourceDefinition = {
  sourceDefinitionId: "unsupported-source-id",
  name: "Supported source",
} as ConnectorDefinitionOrEnterpriseStub;
const supportedDestinationDefinition = {
  destinationDefinitionId: "supported-destination-id",
  name: "Supported destination",
} as ConnectorDefinitionOrEnterpriseStub;
const unsupportedDestinationDefinition = {
  destinationDefinitionId: "unsupported-destination-id",
  name: "Unsupported destination",
} as ConnectorDefinitionOrEnterpriseStub;
const enterpriseStub = {
  id: "enterprise-id",
  name: "Enterprise",
  isEnterprise: true,
} as ConnectorDefinitionOrEnterpriseStub;

const renderBadges = (definition: ConnectorDefinitionOrEnterpriseStub) =>
  render(
    <IntlProvider locale="en" messages={messages}>
      <ConnectorDefinitionBadges definition={definition} />
    </IntlProvider>
  );

describe("ConnectorDefinitionBadges", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockUseAgentsSupportedSourceDefinitionIds.mockReturnValue(new Set(["supported-source-id"]));
    mockUseAgentsSupportedDestinationDefinitionIds.mockReturnValue(new Set(["supported-destination-id"]));
    mockUseExperiment.mockReturnValue(true);
    mockUseIsAdpOrganization.mockReturnValue(false);
    mockUseIsCloudApp.mockReturnValue(true);
  });

  it.each([supportedSourceDefinition, supportedDestinationDefinition])(
    "renders Data replication followed by Agent for a supported regular definition",
    (definition) => {
      renderBadges(definition);

      expect(screen.getAllByText(/Data replication|Agent/).map((badge) => badge.textContent)).toEqual([
        "Data replication",
        "Agent",
      ]);
      expect(screen.getByText("Data replication")).toHaveClass("badge--purple", "badge--radius2xs");
      expect(screen.getByText("Agent")).toHaveClass("badge--coral", "badge--radius2xs");
      expect(screen.getByText("Agent")).not.toHaveClass("badge--uppercase");
    }
  );

  it.each([sameNameUnsupportedSourceDefinition, unsupportedDestinationDefinition])(
    "renders only Data replication for an unsupported regular definition",
    (definition) => {
      renderBadges(definition);

      expect(screen.getByText("Data replication")).toBeInTheDocument();
      expect(screen.queryByText("Agent")).not.toBeInTheDocument();
    }
  );

  it("renders Data replication but not Agent for an enterprise stub", () => {
    renderBadges(enterpriseStub);

    expect(screen.getByText("Data replication")).toBeInTheDocument();
    expect(screen.queryByText("Agent")).not.toBeInTheDocument();
  });

  it("renders in Cloud when the organization is agentic", () => {
    mockUseExperiment.mockReturnValue(false);
    mockUseIsAdpOrganization.mockReturnValue(true);

    renderBadges(supportedSourceDefinition);

    expect(screen.getByText("Data replication")).toBeInTheDocument();
    expect(screen.getByText("Agent")).toBeInTheDocument();
  });

  it.each([
    { isCloudApp: false, optInEnabled: true, isAdpOrganization: true },
    { isCloudApp: true, optInEnabled: false, isAdpOrganization: false },
  ])("does not render when the shared Fusion gate is disabled", ({ isCloudApp, optInEnabled, isAdpOrganization }) => {
    mockUseIsCloudApp.mockReturnValue(isCloudApp);
    mockUseExperiment.mockReturnValue(optInEnabled);
    mockUseIsAdpOrganization.mockReturnValue(isAdpOrganization);

    renderBadges(supportedSourceDefinition);

    expect(screen.queryByText("Data replication")).not.toBeInTheDocument();
    expect(screen.queryByText("Agent")).not.toBeInTheDocument();
  });
});
