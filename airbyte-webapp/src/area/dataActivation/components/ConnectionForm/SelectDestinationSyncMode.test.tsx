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
import { DestinationSyncMode } from "core/api/types/AirbyteClient";

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
          fields: [],
        }),
      ],
    });
  });

  it("unsets mapped destination field names when the new operation does not include them", async () => {
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
          // The destination operation does not include "Some destination field", so it should be unset
          fields: [{ sourceFieldName: "Some source field", destinationFieldName: "" }],
        }),
      ],
    });
  });
});
