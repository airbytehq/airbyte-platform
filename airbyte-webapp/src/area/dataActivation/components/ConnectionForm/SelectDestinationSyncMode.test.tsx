import { screen } from "@testing-library/react";
import { userEvent } from "@testing-library/user-event";
import { PropsWithChildren } from "react";
import { FormProvider, useForm } from "react-hook-form";

import { render } from "test-utils";
import {
  ALL_MOCK_OPERATIONS,
  MOCK_OPERATION_NO_REQUIRED_FIELD,
  MOCK_OPERATION_WITH_REQUIRED_FIELD,
} from "test-utils/mock-data/mockDestinationOperations";

import { DataActivationConnectionFormValues, DataActivationField } from "area/dataActivation/types";
import { DestinationOperation, DestinationSyncMode } from "core/api/types/AirbyteClient";

import { SelectDestinationSyncMode } from "./SelectDestinationSyncMode";

interface MockFormProviderProps {
  destinationObjectName?: string;
  destinationSyncMode?: DestinationSyncMode | null;
  fields?: DataActivationField[];
  onSubmit: (values: DataActivationConnectionFormValues) => void;
}
const MockFormProvider: React.FC<PropsWithChildren<MockFormProviderProps>> = ({
  children,
  destinationObjectName = "test_object",
  destinationSyncMode = null,
  fields = [],
  onSubmit,
}) => {
  const methods = useForm<DataActivationConnectionFormValues>({
    defaultValues: {
      streams: [
        {
          sourceStreamDescriptor: { name: "test_stream", namespace: "test_namespace" },
          destinationObjectName,
          fields,
          sourceSyncMode: "full_refresh",
          destinationSyncMode,
        },
      ],
    },
  });

  return (
    <FormProvider {...methods}>
      {/* Only pass the first arg to make asserting with jest easier */}
      <form onSubmit={methods.handleSubmit((values) => onSubmit(values))}>
        {children}
        <button type="submit">Submit</button>
      </form>
    </FormProvider>
  );
};

describe(`${SelectDestinationSyncMode.name}`, () => {
  it("placeholder is shown", async () => {
    await render(
      <MockFormProvider onSubmit={jest.fn()}>
        <SelectDestinationSyncMode streamIndex={0} destinationCatalog={{ operations: ALL_MOCK_OPERATIONS }} />
      </MockFormProvider>
    );
    expect(screen.getByText("Select insertion method")).toBeInTheDocument();
  });

  it("sets required fields when sync mode is selected", async () => {
    const mockSubmit = jest.fn();

    await render(
      <MockFormProvider destinationObjectName={MOCK_OPERATION_WITH_REQUIRED_FIELD.objectName} onSubmit={mockSubmit}>
        <SelectDestinationSyncMode
          streamIndex={0}
          destinationCatalog={{
            operations: ALL_MOCK_OPERATIONS,
          }}
        />
      </MockFormProvider>
    );

    // Select a sync mode
    await userEvent.click(screen.getByText("Select insertion method"));
    expect(screen.getByText("Insert")).toBeInTheDocument();
    await userEvent.click(screen.getByText("Insert"));

    // Submit the form
    await userEvent.click(screen.getByText("Submit"));

    expect(mockSubmit).toHaveBeenCalledWith({
      streams: [
        expect.objectContaining({
          fields: [
            { destinationFieldName: "ParentId", sourceFieldName: "" },
            { destinationFieldName: "UserOrGroupId", sourceFieldName: "" },
            { destinationFieldName: "AccessLevel", sourceFieldName: "" },
          ],
          matchingKeys: null,
        }),
      ],
    });
  });

  it("does not populate required fields when the operation has none", async () => {
    const mockSubmit = jest.fn();
    await render(
      <MockFormProvider destinationObjectName={MOCK_OPERATION_NO_REQUIRED_FIELD.objectName} onSubmit={mockSubmit}>
        <SelectDestinationSyncMode
          streamIndex={0}
          destinationCatalog={{
            operations: ALL_MOCK_OPERATIONS,
          }}
        />
      </MockFormProvider>
    );

    // Select a sync mode
    await userEvent.click(screen.getByText("Select insertion method"));
    expect(screen.getByText("Insert")).toBeInTheDocument();
    await userEvent.click(screen.getByText("Insert"));

    // Submit the form
    await userEvent.click(screen.getByText("Submit"));

    expect(mockSubmit).toHaveBeenCalledWith({
      streams: [
        expect.objectContaining({
          fields: [{ sourceFieldName: "", destinationFieldName: "" }], // EMPTY_FIELD added when no required fields
        }),
      ],
    });
  });

  it("removes field mappings when the new operation does not require them", async () => {
    const mockSubmit = jest.fn();
    await render(
      <MockFormProvider
        destinationObjectName={MOCK_OPERATION_NO_REQUIRED_FIELD.objectName}
        fields={[{ sourceFieldName: "Some source field", destinationFieldName: "Some destination field" }]}
        onSubmit={mockSubmit}
      >
        <SelectDestinationSyncMode
          streamIndex={0}
          destinationCatalog={{
            operations: ALL_MOCK_OPERATIONS,
          }}
        />
      </MockFormProvider>
    );

    // Select a sync mode
    await userEvent.click(screen.getByText("Select insertion method"));
    expect(screen.getByText("Insert")).toBeInTheDocument();
    await userEvent.click(screen.getByText("Insert"));

    // Submit the form
    await userEvent.click(screen.getByText("Submit"));

    expect(mockSubmit).toHaveBeenCalledWith({
      streams: [
        expect.objectContaining({
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
        <MockFormProvider destinationObjectName={MOCK_OPERATION_NO_MATCHING_KEYS.objectName} onSubmit={mockSubmit}>
          <SelectDestinationSyncMode
            streamIndex={0}
            destinationCatalog={{
              operations: [MOCK_OPERATION_NO_MATCHING_KEYS],
            }}
          />
        </MockFormProvider>
      );

      await userEvent.click(screen.getByText("Select insertion method"));
      await userEvent.click(screen.getByText("Upsert"));

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
        <MockFormProvider destinationObjectName={MOCK_OPERATION_EMPTY_MATCHING_KEYS.objectName} onSubmit={mockSubmit}>
          <SelectDestinationSyncMode
            streamIndex={0}
            destinationCatalog={{
              operations: [MOCK_OPERATION_EMPTY_MATCHING_KEYS],
            }}
          />
        </MockFormProvider>
      );

      await userEvent.click(screen.getByText("Select insertion method"));
      await userEvent.click(screen.getByText("Upsert"));

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
        <MockFormProvider destinationObjectName={MOCK_OPERATION_SINGLE_MATCHING_KEY.objectName} onSubmit={mockSubmit}>
          <SelectDestinationSyncMode
            streamIndex={0}
            destinationCatalog={{
              operations: [MOCK_OPERATION_SINGLE_MATCHING_KEY],
            }}
          />
        </MockFormProvider>
      );

      await userEvent.click(screen.getByText("Select insertion method"));
      await userEvent.click(screen.getByText("Upsert"));

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
        <MockFormProvider
          destinationObjectName={MOCK_OPERATION_MULTIPLE_MATCHING_KEYS.objectName}
          onSubmit={mockSubmit}
        >
          <SelectDestinationSyncMode
            streamIndex={0}
            destinationCatalog={{
              operations: [MOCK_OPERATION_MULTIPLE_MATCHING_KEYS],
            }}
          />
        </MockFormProvider>
      );

      await userEvent.click(screen.getByText("Select insertion method"));
      await userEvent.click(screen.getByText("Upsert"));

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

  it("adds EMPTY_FIELD when operation has no required fields and multiple matching key options", async () => {
    const MOCK_OPERATION_NO_REQUIRED_FIELDS_MULTIPLE_KEYS: DestinationOperation = {
      objectName: "NoRequiredFieldsMultipleKeys",
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
      <MockFormProvider
        destinationObjectName={MOCK_OPERATION_NO_REQUIRED_FIELDS_MULTIPLE_KEYS.objectName}
        fields={[]} // Start with empty fields
        onSubmit={mockSubmit}
      >
        <SelectDestinationSyncMode
          streamIndex={0}
          destinationCatalog={{
            operations: [MOCK_OPERATION_NO_REQUIRED_FIELDS_MULTIPLE_KEYS],
          }}
        />
      </MockFormProvider>
    );

    // Select a sync mode
    await userEvent.click(screen.getByText("Select insertion method"));
    await userEvent.click(screen.getByText("Upsert"));

    // Submit the form
    await userEvent.click(screen.getByText("Submit"));

    expect(mockSubmit).toHaveBeenCalledWith({
      streams: [
        expect.objectContaining({
          fields: [{ sourceFieldName: "", destinationFieldName: "" }], // EMPTY_FIELD added
          matchingKeys: [],
        }),
      ],
    });
  });
});
