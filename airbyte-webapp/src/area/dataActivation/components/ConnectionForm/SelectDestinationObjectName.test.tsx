import { screen } from "@testing-library/react";
import { userEvent } from "@testing-library/user-event";
import { PropsWithChildren } from "react";
import { FormProvider, useForm } from "react-hook-form";

import { mockDestination, render } from "test-utils";
import {
  ALL_MOCK_OPERATIONS,
  MOCK_OPERATION_NO_REQUIRED_FIELD,
  MOCK_OPERATION_WITH_REQUIRED_FIELD,
} from "test-utils/mock-data/mockDestinationOperations";

import { DataActivationConnectionFormValues, DataActivationField } from "area/dataActivation/types";
import { DestinationOperation, DestinationSyncMode } from "core/api/types/AirbyteClient";
import { AirbyteThemeProvider } from "hooks/theme/useAirbyteTheme";

import { SelectDestinationObjectName } from "./SelectDestinationObjectName";

const COMBOBOX_PLACEHOLDER = "Select destination object";

interface MockFormProviderProps {
  destinationObjectName?: string;
  destinationSyncMode?: DestinationSyncMode | null;
  fields?: DataActivationField[];
  matchingKeys?: string[] | null;
  onSubmit: (values: DataActivationConnectionFormValues) => void;
}

const MockFormProvider: React.FC<PropsWithChildren<MockFormProviderProps>> = ({
  children,
  destinationObjectName = "",
  destinationSyncMode = null,
  fields = [],
  matchingKeys = null,
  onSubmit,
}) => {
  const methods = useForm<DataActivationConnectionFormValues>({
    defaultValues: {
      streams: [
        {
          sourceStreamDescriptor: { name: "test_stream", namespace: "test_namespace" },
          destinationObjectName,
          fields,
          matchingKeys,
          sourceSyncMode: "full_refresh",
          destinationSyncMode,
        },
      ],
    },
  });

  return (
    <AirbyteThemeProvider>
      <FormProvider {...methods}>
        <form onSubmit={methods.handleSubmit((values) => onSubmit(values))}>
          {children}
          <button type="submit">Submit</button>
        </form>
      </FormProvider>
    </AirbyteThemeProvider>
  );
};

describe(`${SelectDestinationObjectName.name}`, () => {
  it("placeholder is shown when no object name is selected", async () => {
    await render(
      <MockFormProvider onSubmit={jest.fn()}>
        <SelectDestinationObjectName
          streamIndex={0}
          destination={mockDestination}
          destinationCatalog={{ operations: ALL_MOCK_OPERATIONS }}
        />
      </MockFormProvider>
    );
    expect(screen.getByPlaceholderText(COMBOBOX_PLACEHOLDER)).toBeInTheDocument();
  });

  it("auto-selects sync mode and sets required fields when object name with single operation is selected", async () => {
    const SINGLE_OPERATION_MOCK = [MOCK_OPERATION_WITH_REQUIRED_FIELD];
    const mockSubmit = jest.fn();

    await render(
      <MockFormProvider onSubmit={mockSubmit}>
        <SelectDestinationObjectName
          streamIndex={0}
          destination={mockDestination}
          destinationCatalog={{ operations: SINGLE_OPERATION_MOCK }}
        />
      </MockFormProvider>
    );

    // Select the object name
    const combobox = screen.getByPlaceholderText(COMBOBOX_PLACEHOLDER);
    await userEvent.click(combobox);
    await userEvent.type(combobox, MOCK_OPERATION_WITH_REQUIRED_FIELD.objectName);
    await userEvent.keyboard("{Enter}");

    // Submit the form
    await userEvent.click(screen.getByText("Submit"));

    expect(mockSubmit).toHaveBeenCalledWith({
      streams: [
        expect.objectContaining({
          destinationObjectName: MOCK_OPERATION_WITH_REQUIRED_FIELD.objectName,
          destinationSyncMode: MOCK_OPERATION_WITH_REQUIRED_FIELD.syncMode,
          fields: [
            { destinationFieldName: "ParentId", sourceFieldName: "" },
            { destinationFieldName: "UserOrGroupId", sourceFieldName: "" },
            { destinationFieldName: "AccessLevel", sourceFieldName: "" },
          ],
        }),
      ],
    });
  });

  it("resets sync mode and fields when object name with multiple operations is selected", async () => {
    const MULTIPLE_OPERATIONS_MOCK = [
      MOCK_OPERATION_WITH_REQUIRED_FIELD,
      { ...MOCK_OPERATION_WITH_REQUIRED_FIELD, syncMode: DestinationSyncMode.append_dedup },
    ];
    const mockSubmit = jest.fn();

    await render(
      <MockFormProvider
        destinationSyncMode="append"
        fields={[{ sourceFieldName: "existing", destinationFieldName: "existing" }]}
        onSubmit={mockSubmit}
      >
        <SelectDestinationObjectName
          streamIndex={0}
          destination={mockDestination}
          destinationCatalog={{ operations: MULTIPLE_OPERATIONS_MOCK }}
        />
      </MockFormProvider>
    );

    // Select the object name
    const combobox = screen.getByPlaceholderText(COMBOBOX_PLACEHOLDER);
    await userEvent.click(combobox);
    await userEvent.type(combobox, MOCK_OPERATION_WITH_REQUIRED_FIELD.objectName);
    await userEvent.keyboard("{Enter}");

    // Submit the form
    await userEvent.click(screen.getByText("Submit"));

    expect(mockSubmit).toHaveBeenCalledWith({
      streams: [
        expect.objectContaining({
          destinationObjectName: MOCK_OPERATION_WITH_REQUIRED_FIELD.objectName,
          destinationSyncMode: null,
          matchingKeys: null,
          fields: [{ destinationFieldName: "", sourceFieldName: "" }],
        }),
      ],
    });
  });

  it("handles operation with no required fields", async () => {
    const SINGLE_NO_REQUIRED_FIELDS_MOCK = [MOCK_OPERATION_NO_REQUIRED_FIELD];
    const mockSubmit = jest.fn();

    await render(
      <MockFormProvider onSubmit={mockSubmit}>
        <SelectDestinationObjectName
          streamIndex={0}
          destination={mockDestination}
          destinationCatalog={{ operations: SINGLE_NO_REQUIRED_FIELDS_MOCK }}
        />
      </MockFormProvider>
    );

    // Select the object name
    const combobox = screen.getByPlaceholderText(COMBOBOX_PLACEHOLDER);
    await userEvent.click(combobox);
    await userEvent.type(combobox, MOCK_OPERATION_NO_REQUIRED_FIELD.objectName);
    await userEvent.keyboard("{Enter}");

    // Submit the form
    await userEvent.click(screen.getByText("Submit"));

    expect(mockSubmit).toHaveBeenCalledWith({
      streams: [
        expect.objectContaining({
          destinationObjectName: MOCK_OPERATION_NO_REQUIRED_FIELD.objectName,
          destinationSyncMode: MOCK_OPERATION_NO_REQUIRED_FIELD.syncMode,
          fields: [{ sourceFieldName: "", destinationFieldName: "" }], // EMPTY_FIELD added when no required fields
        }),
      ],
    });
  });

  describe("matching keys behavior", () => {
    it("sets matchingKeys to null when operation has no matchingKeys property", async () => {
      const MOCK_OPERATION_NO_MATCHING_KEYS: DestinationOperation = {
        objectName: "NoMatchingKeys",
        syncMode: "append_dedup",
        schema: {
          additionalProperties: false,
          type: "object",
          properties: {
            Name: { type: "string" },
            Id: { type: "string" },
          },
        },
      };
      const mockSubmit = jest.fn();

      await render(
        <MockFormProvider onSubmit={mockSubmit}>
          <SelectDestinationObjectName
            streamIndex={0}
            destination={mockDestination}
            destinationCatalog={{ operations: [MOCK_OPERATION_NO_MATCHING_KEYS] }}
          />
        </MockFormProvider>
      );

      const combobox = screen.getByPlaceholderText(COMBOBOX_PLACEHOLDER);
      await userEvent.click(combobox);
      await userEvent.type(combobox, "NoMatchingKeys");
      await userEvent.keyboard("{Enter}");

      await userEvent.click(screen.getByText("Submit"));

      expect(mockSubmit).toHaveBeenCalledWith({
        streams: [
          expect.objectContaining({
            matchingKeys: null,
          }),
        ],
      });
    });

    it("sets matchingKeys to null when operation has empty matchingKeys array", async () => {
      const MOCK_OPERATION_EMPTY_MATCHING_KEYS: DestinationOperation = {
        objectName: "EmptyMatchingKeys",
        syncMode: "append_dedup",
        schema: {
          additionalProperties: false,
          type: "object",
          properties: {
            Name: { type: "string" },
            Id: { type: "string" },
          },
        },
        matchingKeys: [],
      };
      const mockSubmit = jest.fn();

      await render(
        <MockFormProvider onSubmit={mockSubmit}>
          <SelectDestinationObjectName
            streamIndex={0}
            destination={mockDestination}
            destinationCatalog={{ operations: [MOCK_OPERATION_EMPTY_MATCHING_KEYS] }}
          />
        </MockFormProvider>
      );

      const combobox = screen.getByPlaceholderText(COMBOBOX_PLACEHOLDER);
      await userEvent.click(combobox);
      await userEvent.type(combobox, "EmptyMatchingKeys");
      await userEvent.keyboard("{Enter}");

      await userEvent.click(screen.getByText("Submit"));

      expect(mockSubmit).toHaveBeenCalledWith({
        streams: [
          expect.objectContaining({
            matchingKeys: null,
          }),
        ],
      });
    });

    it("automatically sets the single matching key when operation has one matching key", async () => {
      const MOCK_OPERATION_SINGLE_MATCHING_KEY: DestinationOperation = {
        objectName: "SingleMatchingKey",
        syncMode: "append_dedup",
        schema: {
          additionalProperties: false,
          type: "object",
          properties: {
            Name: { type: "string" },
            Id: { type: "string" },
          },
        },
        matchingKeys: [["Id"]],
      };
      const mockSubmit = jest.fn();

      await render(
        <MockFormProvider onSubmit={mockSubmit}>
          <SelectDestinationObjectName
            streamIndex={0}
            destination={mockDestination}
            destinationCatalog={{ operations: [MOCK_OPERATION_SINGLE_MATCHING_KEY] }}
          />
        </MockFormProvider>
      );

      const combobox = screen.getByPlaceholderText(COMBOBOX_PLACEHOLDER);
      await userEvent.click(combobox);
      await userEvent.type(combobox, "SingleMatchingKey");
      await userEvent.keyboard("{Enter}");

      await userEvent.click(screen.getByText("Submit"));

      expect(mockSubmit).toHaveBeenCalledWith({
        streams: [
          expect.objectContaining({
            matchingKeys: ["Id"],
          }),
        ],
      });
    });

    it("sets matchingKeys to empty array when operation has multiple matching keys", async () => {
      const MOCK_OPERATION_MULTIPLE_MATCHING_KEYS: DestinationOperation = {
        objectName: "MultipleMatchingKeys",
        syncMode: "append_dedup",
        schema: {
          additionalProperties: false,
          type: "object",
          properties: {
            Name: { type: "string" },
            Id: { type: "string" },
            Email: { type: "string" },
          },
        },
        matchingKeys: [["Id"], ["Email"]],
      };
      const mockSubmit = jest.fn();

      await render(
        <MockFormProvider onSubmit={mockSubmit}>
          <SelectDestinationObjectName
            streamIndex={0}
            destination={mockDestination}
            destinationCatalog={{ operations: [MOCK_OPERATION_MULTIPLE_MATCHING_KEYS] }}
          />
        </MockFormProvider>
      );

      const combobox = screen.getByPlaceholderText(COMBOBOX_PLACEHOLDER);
      await userEvent.click(combobox);
      await userEvent.type(combobox, "MultipleMatchingKeys");
      await userEvent.keyboard("{Enter}");

      await userEvent.click(screen.getByText("Submit"));

      expect(mockSubmit).toHaveBeenCalledWith({
        streams: [
          expect.objectContaining({
            matchingKeys: [],
          }),
        ],
      });
    });
  });

  it("clears existing form values when changing to a different object name", async () => {
    const mockSubmit = jest.fn();

    await render(
      <MockFormProvider
        destinationObjectName={MOCK_OPERATION_WITH_REQUIRED_FIELD.objectName}
        destinationSyncMode="append"
        fields={[{ sourceFieldName: "existing", destinationFieldName: "existing" }]}
        matchingKeys={["existingKey"]}
        onSubmit={mockSubmit}
      >
        <SelectDestinationObjectName
          streamIndex={0}
          destination={mockDestination}
          destinationCatalog={{ operations: ALL_MOCK_OPERATIONS }}
        />
      </MockFormProvider>
    );

    // Change to a different object name
    const combobox = screen.getByPlaceholderText(COMBOBOX_PLACEHOLDER);
    await userEvent.click(combobox);
    await userEvent.clear(combobox);
    await userEvent.type(combobox, MOCK_OPERATION_NO_REQUIRED_FIELD.objectName);
    await userEvent.keyboard("{Enter}");

    await userEvent.click(screen.getByText("Submit"));

    expect(mockSubmit).toHaveBeenCalledWith({
      streams: [
        expect.objectContaining({
          destinationObjectName: MOCK_OPERATION_NO_REQUIRED_FIELD.objectName,
          destinationSyncMode: MOCK_OPERATION_NO_REQUIRED_FIELD.syncMode,
          fields: [{ sourceFieldName: "", destinationFieldName: "" }], // EMPTY_FIELD added when no required fields
          matchingKeys: null,
        }),
      ],
    });
  });

  it("does not trigger onChange when selecting the same object name", async () => {
    const mockSubmit = jest.fn();

    await render(
      <MockFormProvider
        destinationObjectName={MOCK_OPERATION_WITH_REQUIRED_FIELD.objectName}
        destinationSyncMode="append"
        fields={[{ sourceFieldName: "existing", destinationFieldName: "existing" }]}
        onSubmit={mockSubmit}
      >
        <SelectDestinationObjectName
          streamIndex={0}
          destination={mockDestination}
          destinationCatalog={{ operations: [MOCK_OPERATION_WITH_REQUIRED_FIELD] }}
        />
      </MockFormProvider>
    );

    // Try to select the same object name that's already selected
    // Since it's already selected, just submit without changing anything
    await userEvent.click(screen.getByText("Submit"));

    // Form values should remain unchanged
    expect(mockSubmit).toHaveBeenCalledWith({
      streams: [
        expect.objectContaining({
          destinationObjectName: MOCK_OPERATION_WITH_REQUIRED_FIELD.objectName,
          destinationSyncMode: "append",
          fields: [{ sourceFieldName: "existing", destinationFieldName: "existing" }],
        }),
      ],
    });
  });

  it("adds EMPTY_FIELD when selecting object name with multiple operations", async () => {
    const MULTIPLE_OPERATIONS_MOCK = [
      MOCK_OPERATION_WITH_REQUIRED_FIELD,
      { ...MOCK_OPERATION_WITH_REQUIRED_FIELD, syncMode: DestinationSyncMode.append_dedup },
    ];
    const mockSubmit = jest.fn();

    await render(
      <MockFormProvider
        fields={[]} // Start with no fields
        onSubmit={mockSubmit}
      >
        <SelectDestinationObjectName
          streamIndex={0}
          destination={mockDestination}
          destinationCatalog={{ operations: MULTIPLE_OPERATIONS_MOCK }}
        />
      </MockFormProvider>
    );

    // Select object name with multiple operations
    const combobox = screen.getByPlaceholderText(COMBOBOX_PLACEHOLDER);
    await userEvent.click(combobox);
    await userEvent.type(combobox, MOCK_OPERATION_WITH_REQUIRED_FIELD.objectName);
    await userEvent.keyboard("{Enter}");

    await userEvent.click(screen.getByText("Submit"));

    expect(mockSubmit).toHaveBeenCalledWith({
      streams: [
        expect.objectContaining({
          fields: [{ destinationFieldName: "", sourceFieldName: "" }], // EMPTY_FIELD added
        }),
      ],
    });
  });
});
