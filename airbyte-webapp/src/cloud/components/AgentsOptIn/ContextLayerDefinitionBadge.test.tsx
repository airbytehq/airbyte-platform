import { render, screen } from "@testing-library/react";
import { IntlProvider } from "react-intl";

import { ConnectorIds } from "area/connector/utils/constants";
import { useAgentsSupportedDestinationDefinitionIds, useAgentsSupportedSourceDefinitionIds } from "core/api";
import { ConnectorDefinitionOrEnterpriseStub } from "core/domain/connector";
import { useIsCloudApp } from "core/utils/app";

import { ContextLayerDefinitionBadge } from "./ContextLayerDefinitionBadge";
import { useShowAgentsOptIn } from "./useShowAgentsOptIn";

jest.mock("core/api", () => ({
  useAgentsSupportedDestinationDefinitionIds: jest.fn(),
  useAgentsSupportedSourceDefinitionIds: jest.fn(),
}));

jest.mock("core/utils/app", () => ({
  useIsCloudApp: jest.fn(),
}));

jest.mock("./useShowAgentsOptIn", () => ({
  useShowAgentsOptIn: jest.fn(),
}));

const mockUseAgentsSupportedDestinationDefinitionIds =
  useAgentsSupportedDestinationDefinitionIds as jest.MockedFunction<typeof useAgentsSupportedDestinationDefinitionIds>;
const mockUseAgentsSupportedSourceDefinitionIds = useAgentsSupportedSourceDefinitionIds as jest.MockedFunction<
  typeof useAgentsSupportedSourceDefinitionIds
>;
const mockUseIsCloudApp = useIsCloudApp as jest.MockedFunction<typeof useIsCloudApp>;
const mockUseShowAgentsOptIn = useShowAgentsOptIn as jest.MockedFunction<typeof useShowAgentsOptIn>;

const messages = {
  "cloud.contextLayer.badge": "Context layer",
};

const sourceDefinition = {
  sourceDefinitionId: ConnectorIds.Sources.GitHub,
  name: "GitHub",
} as ConnectorDefinitionOrEnterpriseStub;
const postgresNamedGithubDefinition = {
  sourceDefinitionId: ConnectorIds.Sources.Postgres,
  name: "GitHub",
} as ConnectorDefinitionOrEnterpriseStub;
const unsupportedSourceDefinition = {
  sourceDefinitionId: "source-id",
  name: "Unsupported",
} as ConnectorDefinitionOrEnterpriseStub;
const snowflakeDefinition = {
  destinationDefinitionId: ConnectorIds.Destinations.Snowflake,
  name: "Snowflake",
} as ConnectorDefinitionOrEnterpriseStub;
const unsupportedDestinationDefinition = {
  destinationDefinitionId: "destination-id",
  name: "Unsupported",
} as ConnectorDefinitionOrEnterpriseStub;
const enterpriseStub = {
  id: "enterprise-id",
  name: "Enterprise",
  isEnterprise: true,
} as ConnectorDefinitionOrEnterpriseStub;

const renderBadge = (definition: ConnectorDefinitionOrEnterpriseStub) =>
  render(
    <IntlProvider locale="en" messages={messages}>
      <ContextLayerDefinitionBadge definition={definition} />
    </IntlProvider>
  );

describe("ContextLayerDefinitionBadge", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockUseAgentsSupportedSourceDefinitionIds.mockReturnValue(new Set([ConnectorIds.Sources.GitHub]));
    mockUseAgentsSupportedDestinationDefinitionIds.mockReturnValue(new Set([ConnectorIds.Destinations.Snowflake]));
    mockUseIsCloudApp.mockReturnValue(true);
    mockUseShowAgentsOptIn.mockReturnValue(true);
  });

  it("renders for a supported source definition", () => {
    renderBadge(sourceDefinition);
    expect(screen.getByText("Context layer")).toBeInTheDocument();
  });

  it("does not render for an unsupported source definition", () => {
    renderBadge(unsupportedSourceDefinition);
    expect(screen.queryByText("Context layer")).not.toBeInTheDocument();
  });

  it("does not render for a source with a supported name but unsupported definition ID", () => {
    renderBadge(postgresNamedGithubDefinition);
    expect(screen.queryByText("Context layer")).not.toBeInTheDocument();
  });

  it("renders for a supported Snowflake destination definition", () => {
    renderBadge(snowflakeDefinition);
    expect(screen.getByText("Context layer")).toBeInTheDocument();
  });

  it("does not render for an unsupported destination definition or enterprise stub", () => {
    renderBadge(unsupportedDestinationDefinition);
    expect(screen.queryByText("Context layer")).not.toBeInTheDocument();

    renderBadge(enterpriseStub);
    expect(screen.queryByText("Context layer")).not.toBeInTheDocument();
  });

  it("does not render outside Cloud or when the gate is disabled", () => {
    mockUseIsCloudApp.mockReturnValue(false);
    renderBadge(sourceDefinition);
    expect(screen.queryByText("Context layer")).not.toBeInTheDocument();

    mockUseIsCloudApp.mockReturnValue(true);
    mockUseShowAgentsOptIn.mockReturnValue(false);
    renderBadge(sourceDefinition);
    expect(screen.queryByText("Context layer")).not.toBeInTheDocument();
  });
});
