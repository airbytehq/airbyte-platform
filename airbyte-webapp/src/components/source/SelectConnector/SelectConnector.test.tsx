import userEvent from "@testing-library/user-event";

import { render } from "test-utils";
import { mockSourceDefinition } from "test-utils/mock-data/mockSource";
import { mockTheme } from "test-utils/mock-data/mockTheme";
import { mockWebappConfig } from "test-utils/mock-data/mockWebappConfig";
import { mockWorkspace } from "test-utils/mock-data/mockWorkspace";

import { SelectConnector } from "./SelectConnector";

const mockTrackSelectConnector = jest.fn();
const mockTrackSelectEnterpriseStub = jest.fn();

jest.mock("./useTrackSelectConnector", () => ({
  useTrackSelectConnector: () => mockTrackSelectConnector,
  useTrackSelectEnterpriseStub: () => mockTrackSelectEnterpriseStub,
}));

jest.mock("hooks/theme/useAirbyteTheme", () => ({
  useAirbyteTheme: () => mockTheme,
}));

jest.mock("core/api", () => ({
  useCurrentWorkspace: () => mockWorkspace,
  useFilters: (defaultFilters: unknown) => {
    return [defaultFilters, () => null];
  },
  useListEnterpriseStubsForWorkspace: () => ({ enterpriseSourceDefinitions: [] }),
  useGetWebappConfig: () => mockWebappConfig,
}));

jest.mock("core/utils/useOrganizationSubscriptionStatus", () => ({
  useOrganizationSubscriptionStatus: () => ({ isInTrial: false }),
}));

jest.mock("hooks/services/Experiment", () => ({
  useExperiment: () => false,
}));

describe(`${SelectConnector.name}`, () => {
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
});
