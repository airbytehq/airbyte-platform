import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { v4 as uuid } from "uuid";

import { render } from "test-utils";
import { mockConnection } from "test-utils/mock-data/mockConnection";

import { RowFilteringMapperConfiguration } from "core/api/types/AirbyteClient";
import messages from "locales/en.json";

import { RowFilteringMapperForm } from "./RowFilteringMapperForm";
import { StreamMapperWithId } from "../types";

const mockMapperId = uuid();
const mockstreamDescriptorKey = "undefined-pokemon";

const mockRowFilteringMapperConfiguration: RowFilteringMapperConfiguration = {
  conditions: {
    type: "EQUAL",
    fieldName: "name",
    comparisonValue: "comparisonValue",
  },
};

const mockMapper: StreamMapperWithId<RowFilteringMapperConfiguration> = {
  id: mockMapperId,
  validationCallback: () => Promise.resolve(true),
  mapperConfiguration: mockRowFilteringMapperConfiguration,
  type: "row-filtering",
};

jest.mock("core/api", () => ({
  useCurrentConnection: () => mockConnection,
}));

jest.mock("hooks/services/ConnectionForm/ConnectionFormService", () => ({
  useConnectionFormService: () => ({
    mode: "edit",
  }),
}));

const mockUpdateLocalMapping = jest.fn();
jest.mock("../MappingContext", () => {
  const originalModule = jest.requireActual("../MappingContext");
  return {
    ...originalModule,
    useMappingContext: () => ({
      updateLocalMapping: mockUpdateLocalMapping,
      validateMappings: jest.fn(),
    }),
  };
});

describe(`${RowFilteringMapperForm.name}`, () => {
  it("renders error messages", async () => {
    await render(<RowFilteringMapperForm mapping={mockMapper} streamDescriptorKey={mockstreamDescriptorKey} />);

    // clear the default values
    const targetFieldInput = screen.getByPlaceholderText(messages["connections.mappings.selectField"]);
    await userEvent.clear(targetFieldInput);
    const comparisonValueInput = screen.getByTestId("comparisonValue");
    await userEvent.clear(comparisonValueInput);
    // blur the input to trigger validation
    await userEvent.type(comparisonValueInput, "[Tab]");

    // Two validation errors should be shown
    await waitFor(() => expect(screen.getAllByText(messages["form.empty.error"])).toHaveLength(2));
  });

  it("updates the mapper context with new configuration values", async () => {
    await render(<RowFilteringMapperForm mapping={mockMapper} streamDescriptorKey={mockstreamDescriptorKey} />);

    const targetFieldInput = screen.getByPlaceholderText(messages["connections.mappings.selectField"]);
    const newTargetFieldName = "location_area_encounters";
    await userEvent.click(targetFieldInput);
    await userEvent.click(screen.getByText(newTargetFieldName));

    const comparisonValueInput = screen.getByTestId("comparisonValue");
    const newComparisonValue = "some new value";
    await userEvent.clear(comparisonValueInput);
    await userEvent.type(comparisonValueInput, `${newComparisonValue}[Tab]`);

    // Form should have been submitted automatically with new config values
    await waitFor(() =>
      expect(mockUpdateLocalMapping).toHaveBeenLastCalledWith(mockstreamDescriptorKey, mockMapperId, {
        mapperConfiguration: {
          conditions: {
            ...mockRowFilteringMapperConfiguration.conditions,
            comparisonValue: newComparisonValue,
            fieldName: newTargetFieldName,
          },
        },
      })
    );
  });
});
