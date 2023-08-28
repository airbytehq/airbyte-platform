import userEvent from "@testing-library/user-event";

import { render } from "test-utils";
import { mockSourceDefinition } from "test-utils/mock-data/mockSource";
import { mockTheme } from "test-utils/mock-data/mockTheme";
import { mockWorkspace } from "test-utils/mock-data/mockWorkspace";

import { SelectConnector } from "./SelectConnector";

const mockTrackSelectConnector = jest.fn();

jest.mock("./useTrackSelectConnector", () => ({
  useTrackSelectConnector: () => mockTrackSelectConnector,
}));

jest.mock("hooks/theme/useAirbyteTheme", () => ({
  useAirbyteTheme: () => mockTheme,
}));

jest.mock("core/api", () => ({
  useCurrentWorkspace: () => mockWorkspace,
}));

describe(`${SelectConnector.name}`, () => {
  it("Tracks an analytics event when a connector is selected", async () => {
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
  });
});
