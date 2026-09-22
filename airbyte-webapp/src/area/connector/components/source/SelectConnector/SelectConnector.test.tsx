import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { render } from "test-utils";
import { mockDestinationDefinition } from "test-utils/mock-data/mockDestination";
import { mockSourceDefinition } from "test-utils/mock-data/mockSource";
import { mockTheme } from "test-utils/mock-data/mockTheme";
import { mockWebappConfig } from "test-utils/mock-data/mockWebappConfig";
import { mockWorkspace } from "test-utils/mock-data/mockWorkspace";

import { ConnectorDefinition } from "core/domain/connector";

import { SelectConnector } from "./SelectConnector";

const mockTrackSelectConnector = jest.fn();
const mockTrackSelectEnterpriseStub = jest.fn();
let mockIsUnifiedView = false;
let mockExternalCloudOrgsEnabled = true;
let mockSupportedSourceDefinitionIds = new Set<string>();
let mockSupportedDestinationDefinitionIds = new Set<string>();
let mockSourceDefinitionMap = new Map<string, ConnectorDefinition>();
let mockDestinationDefinitionMap = new Map<string, ConnectorDefinition>();

jest.mock("./useTrackSelectConnector", () => ({
  useTrackSelectConnector: () => mockTrackSelectConnector,
  useTrackSelectEnterpriseStub: () => mockTrackSelectEnterpriseStub,
}));

jest.mock("core/utils/useAirbyteTheme", () => ({
  useAirbyteTheme: () => mockTheme,
}));

jest.mock("core/api", () => ({
  useCurrentWorkspace: () => mockWorkspace,
  useCurrentWorkspaceOrUndefined: () => mockWorkspace,
  useFirstOrg: () => ({ organizationId: mockWorkspace.organizationId }),
  useOrganization: () => undefined,
  useAgentsSupportedSourceDefinitionIds: () => mockSupportedSourceDefinitionIds,
  useAgentsSupportedDestinationDefinitionIds: () => mockSupportedDestinationDefinitionIds,
  useSourceDefinitionList: () => ({ sourceDefinitionMap: mockSourceDefinitionMap }),
  useDestinationDefinitionList: () => ({ destinationDefinitionMap: mockDestinationDefinitionMap }),
  useCurrentOrganizationInfo: () => ({ organizationPlanId: undefined }),
  useFilters: (defaultFilters: Record<string, string | null>) => {
    const { useState } = jest.requireActual<typeof import("react")>("react");
    const [filters, setFilters] = useState(defaultFilters);

    return [
      filters,
      (filterName: string, filterValue: string | null) =>
        setFilters((currentFilters) => ({ ...currentFilters, [filterName]: filterValue })),
    ];
  },
  useListEnterpriseSourceStubs: () => ({ enterpriseSourceDefinitions: [] }),
  useListEnterpriseDestinationStubs: () => ({ enterpriseDestinationDefinitions: [] }),
  useGetWebappConfig: () => mockWebappConfig,
}));

jest.mock("core/utils/useOrganizationSubscriptionStatus", () => ({
  useOrganizationSubscriptionStatus: () => ({ isInTrial: false }),
}));

jest.mock("core/services/Experiment", () => ({
  useExperiment: (experimentId: string) =>
    experimentId === "connector.unifiedConnectorView" ? mockIsUnifiedView : mockExternalCloudOrgsEnabled,
}));

jest.mock("area/organization/utils", () => ({
  ORG_PLAN_IDS: {
    CORE: "plan-airbyte-core",
    FLEX: "plan-airbyte-flex",
    PRO: "plan-airbyte-pro",
    SME: "plan-airbyte-sme",
    STANDARD: "plan-airbyte-standard",
    STANDARD_TRIAL: "plan-airbyte-standard-trial",
  },
  useCurrentOrganizationId: () => mockWorkspace.organizationId,
  useIsAdpOrganization: () => false,
  useOrganizationPlan: () => ({ isStandardPlan: false }),
}));

jest.mock("core/utils/app", () => ({
  useIsCloudApp: () => true,
}));

describe(`${SelectConnector.name}`, () => {
  beforeEach(() => {
    mockIsUnifiedView = false;
    mockExternalCloudOrgsEnabled = true;
    mockSupportedSourceDefinitionIds = new Set<string>();
    mockSupportedDestinationDefinitionIds = new Set<string>();
    mockSourceDefinitionMap = new Map();
    mockDestinationDefinitionMap = new Map();
  });

  it("Tracks an analytics event when a regular connector is selected", async () => {
    const { getByText } = await render(
      <SelectConnector
        connectorType="source"
        connectorDefinitions={[mockSourceDefinition]}
        onSelectConnectorDefinition={jest.fn()}
        suggestedConnectorDefinitionIds={[]}
      />
    );

    const connectorButton = getByText(mockSourceDefinition.name);
    await userEvent.click(connectorButton);

    expect(mockTrackSelectConnector).toHaveBeenCalledTimes(1);
    expect(mockTrackSelectEnterpriseStub).not.toHaveBeenCalled();
  });

  it.each([
    { connectorType: "source" as const, isUnifiedView: false },
    { connectorType: "source" as const, isUnifiedView: true },
    { connectorType: "destination" as const, isUnifiedView: false },
    { connectorType: "destination" as const, isUnifiedView: true },
  ])(
    "filters $connectorType definitions by their Agent badge in the unified=$isUnifiedView view",
    async ({ connectorType, isUnifiedView }) => {
      mockIsUnifiedView = isUnifiedView;
      const agentDefinitionId = `agent-${connectorType}-id`;
      const regularDefinitionId = `regular-${connectorType}-id`;
      const agentDefinitionName = `Agent ${connectorType}`;
      const regularDefinitionName = `Regular ${connectorType}`;
      const enterpriseDefinitionName = `Enterprise ${connectorType}`;
      const connectorDefinitions: ConnectorDefinition[] =
        connectorType === "source"
          ? [
              { ...mockSourceDefinition, sourceDefinitionId: agentDefinitionId, name: agentDefinitionName },
              { ...mockSourceDefinition, sourceDefinitionId: regularDefinitionId, name: regularDefinitionName },
              {
                ...mockSourceDefinition,
                sourceDefinitionId: `enterprise-${connectorType}-id`,
                name: enterpriseDefinitionName,
                enterprise: true,
              },
            ]
          : [
              { ...mockDestinationDefinition, destinationDefinitionId: agentDefinitionId, name: agentDefinitionName },
              {
                ...mockDestinationDefinition,
                destinationDefinitionId: regularDefinitionId,
                name: regularDefinitionName,
              },
              {
                ...mockDestinationDefinition,
                destinationDefinitionId: `enterprise-${connectorType}-id`,
                name: enterpriseDefinitionName,
                enterprise: true,
              },
            ];

      if (connectorType === "source") {
        mockSupportedSourceDefinitionIds = new Set([agentDefinitionId]);
      } else {
        mockSupportedDestinationDefinitionIds = new Set([agentDefinitionId]);
      }

      await render(
        <SelectConnector
          connectorType={connectorType}
          connectorDefinitions={connectorDefinitions}
          onSelectConnectorDefinition={jest.fn()}
          suggestedConnectorDefinitionIds={[]}
        />
      );

      const agentFilter = screen.getByLabelText("Agent connectors");
      expect(agentFilter).toBeChecked();
      expect(screen.getByText(agentDefinitionName)).toBeInTheDocument();
      expect(screen.getByText(regularDefinitionName)).toBeInTheDocument();
      expect(screen.getByText(enterpriseDefinitionName)).toBeInTheDocument();

      await userEvent.click(screen.getByLabelText("Airbyte connectors"));
      await userEvent.click(screen.getByLabelText("Enterprise connectors"));

      expect(screen.getByText(agentDefinitionName)).toBeInTheDocument();
      expect(screen.queryByText(regularDefinitionName)).not.toBeInTheDocument();
      expect(screen.queryByText(enterpriseDefinitionName)).not.toBeInTheDocument();

      await userEvent.click(screen.getByLabelText("Airbyte connectors"));

      await userEvent.click(agentFilter);

      expect(agentFilter).not.toBeChecked();
      expect(screen.queryByText(agentDefinitionName)).not.toBeInTheDocument();
      expect(screen.getByText(regularDefinitionName)).toBeInTheDocument();
      expect(screen.queryByText(enterpriseDefinitionName)).not.toBeInTheDocument();
    }
  );

  it("does not render or apply the Agent filter when the Fusion gate is disabled", async () => {
    mockExternalCloudOrgsEnabled = false;
    const agentDefinition = {
      ...mockSourceDefinition,
      sourceDefinitionId: "agent-source-id",
      name: "Agent source",
    };
    mockSupportedSourceDefinitionIds = new Set([agentDefinition.sourceDefinitionId]);

    await render(
      <SelectConnector
        connectorType="source"
        connectorDefinitions={[agentDefinition]}
        onSelectConnectorDefinition={jest.fn()}
        suggestedConnectorDefinitionIds={[]}
      />
    );

    expect(screen.queryByLabelText("Agent connectors")).not.toBeInTheDocument();
    expect(screen.getByText(agentDefinition.name)).toBeInTheDocument();
  });

  it.each([false, true])("filters suggested cards with the unified=%s view results", async (isUnifiedView) => {
    mockIsUnifiedView = isUnifiedView;
    const agentDefinition = {
      ...mockSourceDefinition,
      sourceDefinitionId: "suggested-agent-source-id",
      name: "Suggested Agent source",
    };
    const regularDefinition = {
      ...mockSourceDefinition,
      sourceDefinitionId: "suggested-regular-source-id",
      name: "Suggested regular source",
    };
    mockSupportedSourceDefinitionIds = new Set([agentDefinition.sourceDefinitionId]);
    mockSourceDefinitionMap = new Map([
      [agentDefinition.sourceDefinitionId, agentDefinition],
      [regularDefinition.sourceDefinitionId, regularDefinition],
    ]);

    await render(
      <SelectConnector
        connectorType="source"
        connectorDefinitions={[agentDefinition, regularDefinition]}
        onSelectConnectorDefinition={jest.fn()}
        suggestedConnectorDefinitionIds={[agentDefinition.sourceDefinitionId, regularDefinition.sourceDefinitionId]}
      />
    );

    await userEvent.click(screen.getByLabelText("Airbyte connectors"));
    await userEvent.click(screen.getByLabelText("Enterprise connectors"));

    expect(screen.queryByText(regularDefinition.name)).not.toBeInTheDocument();
    expect(screen.getAllByText(agentDefinition.name)).toHaveLength(2);
  });

  it("lets the Marketplace tab show only Agent connectors or only regular connectors", async () => {
    const agentDefinition = {
      ...mockSourceDefinition,
      sourceDefinitionId: "agent-marketplace-source-id",
      name: "Agent marketplace source",
      supportLevel: "community" as const,
    };
    const regularDefinition = {
      ...mockSourceDefinition,
      sourceDefinitionId: "regular-marketplace-source-id",
      name: "Regular marketplace source",
      supportLevel: "community" as const,
    };
    mockSupportedSourceDefinitionIds = new Set([agentDefinition.sourceDefinitionId]);

    await render(
      <SelectConnector
        connectorType="source"
        connectorDefinitions={[agentDefinition, regularDefinition]}
        onSelectConnectorDefinition={jest.fn()}
        suggestedConnectorDefinitionIds={[]}
      />
    );

    await userEvent.click(screen.getByRole("button", { name: "Marketplace" }));

    expect(screen.getByLabelText("Marketplace connectors")).toBeChecked();
    const agentFilter = screen.getByLabelText("Agent connectors");
    expect(agentFilter).toBeChecked();
    expect(screen.getByText(agentDefinition.name)).toBeInTheDocument();
    expect(screen.getByText(regularDefinition.name)).toBeInTheDocument();

    await userEvent.click(screen.getByLabelText("Marketplace connectors"));

    expect(screen.getByText(agentDefinition.name)).toBeInTheDocument();
    expect(screen.queryByText(regularDefinition.name)).not.toBeInTheDocument();

    await userEvent.click(screen.getByLabelText("Marketplace connectors"));
    await userEvent.click(agentFilter);

    expect(screen.queryByText(agentDefinition.name)).not.toBeInTheDocument();
    expect(screen.getByText(regularDefinition.name)).toBeInTheDocument();
  });
});
