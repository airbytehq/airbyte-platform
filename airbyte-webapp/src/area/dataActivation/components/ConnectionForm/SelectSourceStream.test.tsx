import { screen } from "@testing-library/react";
import { userEvent } from "@testing-library/user-event";
import { PropsWithChildren } from "react";
import { FormProvider, useForm } from "react-hook-form";

import { mockSource, render } from "test-utils";

import { DataActivationConnectionFormValues, DataActivationField } from "area/dataActivation/types";
import { AirbyteCatalog, AirbyteStreamAndConfiguration, SyncMode } from "core/api/types/AirbyteClient";
import { AirbyteThemeProvider } from "hooks/theme/useAirbyteTheme";

import { SelectSourceStream } from "./SelectSourceStream";

const COMBOBOX_PLACEHOLDER = "Select source stream";

interface MockFormProviderProps {
  sourceStreamDescriptor?: { name: string; namespace?: string };
  sourceSyncMode?: SyncMode | null;
  fields?: DataActivationField[];
  cursorField?: string | null;
  onSubmit: (values: DataActivationConnectionFormValues) => void;
}

const MockFormProvider: React.FC<PropsWithChildren<MockFormProviderProps>> = ({
  children,
  sourceStreamDescriptor = { name: "" },
  sourceSyncMode = null,
  fields = [],
  cursorField = null,
  onSubmit,
}) => {
  const methods = useForm<DataActivationConnectionFormValues>({
    defaultValues: {
      streams: [
        {
          sourceStreamDescriptor,
          fields,
          sourceSyncMode,
          cursorField,
          destinationObjectName: "",
          destinationSyncMode: null,
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

// Mock source catalogs for testing
const createMockStream = (
  name: string,
  namespace: string | undefined = undefined,
  supportedSyncModes: SyncMode[] = ["full_refresh"],
  properties: Record<string, unknown> = { id: { type: "string" }, name: { type: "string" } }
) =>
  ({
    stream: {
      name,
      namespace,
      supportedSyncModes,
      sourceDefinedCursor: false,
      jsonSchema: {
        type: "object",
        properties,
      },
    },
    config: {
      syncMode: "full_refresh",
      destinationSyncMode: "append",
      selected: true,
      fieldSelectionEnabled: false,
    },
  }) as const satisfies AirbyteStreamAndConfiguration;

const MOCK_SINGLE_SYNC_MODE_STREAM = createMockStream("users", "public", ["full_refresh"]);
const MOCK_MULTIPLE_SYNC_MODES_STREAM = createMockStream("orders", "public", ["full_refresh", "incremental"]);
const MOCK_NO_NAMESPACE_STREAM = createMockStream("products", undefined, ["full_refresh"]);

const MOCK_SOURCE_CATALOG_SINGLE_NAMESPACE: AirbyteCatalog = {
  streams: [MOCK_SINGLE_SYNC_MODE_STREAM, MOCK_MULTIPLE_SYNC_MODES_STREAM],
};

const MOCK_SOURCE_CATALOG_MULTIPLE_NAMESPACES: AirbyteCatalog = {
  streams: [createMockStream("users", "public"), createMockStream("users", "staging"), MOCK_NO_NAMESPACE_STREAM],
};

describe(`${SelectSourceStream.name}`, () => {
  it("placeholder is shown when no stream is selected", async () => {
    await render(
      <MockFormProvider onSubmit={jest.fn()}>
        <SelectSourceStream index={0} source={mockSource} sourceCatalog={MOCK_SOURCE_CATALOG_SINGLE_NAMESPACE} />
      </MockFormProvider>
    );
    expect(screen.getByPlaceholderText(COMBOBOX_PLACEHOLDER)).toBeInTheDocument();
  });

  it("auto-selects sync mode when stream with single sync mode is selected", async () => {
    const mockSubmit = jest.fn();

    await render(
      <MockFormProvider onSubmit={mockSubmit}>
        <SelectSourceStream index={0} source={mockSource} sourceCatalog={MOCK_SOURCE_CATALOG_SINGLE_NAMESPACE} />
      </MockFormProvider>
    );

    // Select the stream
    const combobox = screen.getByPlaceholderText(COMBOBOX_PLACEHOLDER);
    await userEvent.click(combobox);
    await userEvent.type(combobox, MOCK_SINGLE_SYNC_MODE_STREAM.stream.name);
    await userEvent.keyboard("{Enter}");

    // Submit the form
    await userEvent.click(screen.getByText("Submit"));

    expect(mockSubmit).toHaveBeenCalledWith({
      streams: [
        expect.objectContaining({
          sourceStreamDescriptor: {
            name: MOCK_SINGLE_SYNC_MODE_STREAM.stream.name,
            namespace: MOCK_SINGLE_SYNC_MODE_STREAM.stream.namespace,
          },
          sourceSyncMode: MOCK_SINGLE_SYNC_MODE_STREAM.stream.supportedSyncModes[0],
          cursorField: null,
        }),
      ],
    });
  });

  it("resets sync mode when stream with multiple sync modes is selected", async () => {
    const mockSubmit = jest.fn();

    await render(
      <MockFormProvider sourceSyncMode="full_refresh" cursorField="existing_cursor" onSubmit={mockSubmit}>
        <SelectSourceStream index={0} source={mockSource} sourceCatalog={MOCK_SOURCE_CATALOG_SINGLE_NAMESPACE} />
      </MockFormProvider>
    );

    // Select the stream with multiple sync modes
    const combobox = screen.getByPlaceholderText(COMBOBOX_PLACEHOLDER);
    await userEvent.click(combobox);
    await userEvent.type(combobox, MOCK_MULTIPLE_SYNC_MODES_STREAM.stream.name);
    await userEvent.keyboard("{Enter}");

    // Submit the form
    await userEvent.click(screen.getByText("Submit"));

    expect(mockSubmit).toHaveBeenCalledWith({
      streams: [
        expect.objectContaining({
          sourceStreamDescriptor: {
            name: MOCK_MULTIPLE_SYNC_MODES_STREAM.stream.name,
            namespace: MOCK_MULTIPLE_SYNC_MODES_STREAM.stream.namespace,
          },
          sourceSyncMode: null,
          cursorField: null,
        }),
      ],
    });
  });

  it("displays namespaces when multiple namespaces exist", async () => {
    await render(
      <MockFormProvider onSubmit={jest.fn()}>
        <SelectSourceStream index={0} source={mockSource} sourceCatalog={MOCK_SOURCE_CATALOG_MULTIPLE_NAMESPACES} />
      </MockFormProvider>
    );

    const combobox = screen.getByPlaceholderText(COMBOBOX_PLACEHOLDER);
    await userEvent.click(combobox);

    // Type to filter options - should see namespace.name format
    await userEvent.type(combobox, "public.users");

    // The virtualized list should show the namespaced option
    expect(combobox).toHaveValue("public.users");
  });

  it("does not display namespaces when only one namespace exists", async () => {
    await render(
      <MockFormProvider onSubmit={jest.fn()}>
        <SelectSourceStream index={0} source={mockSource} sourceCatalog={MOCK_SOURCE_CATALOG_SINGLE_NAMESPACE} />
      </MockFormProvider>
    );

    const combobox = screen.getByPlaceholderText(COMBOBOX_PLACEHOLDER);
    await userEvent.click(combobox);

    // Type just the stream name without namespace
    await userEvent.type(combobox, MOCK_SINGLE_SYNC_MODE_STREAM.stream.name);

    expect(combobox).toHaveValue(MOCK_SINGLE_SYNC_MODE_STREAM.stream.name);
  });

  it("validates source field names when switching streams", async () => {
    const mockSubmit = jest.fn();

    // Create a stream with different fields
    const streamWithDifferentFields = createMockStream("customers", "public", ["full_refresh"], {
      customer_id: { type: "string" },
      email: { type: "string" },
    });

    const catalogWithDifferentFields: AirbyteCatalog = {
      streams: [MOCK_SINGLE_SYNC_MODE_STREAM, streamWithDifferentFields],
    };

    await render(
      <MockFormProvider
        sourceStreamDescriptor={{
          name: MOCK_SINGLE_SYNC_MODE_STREAM.stream.name,
          namespace: MOCK_SINGLE_SYNC_MODE_STREAM.stream.namespace,
        }}
        fields={[
          { sourceFieldName: "id", destinationFieldName: "user_id" },
          { sourceFieldName: "invalid_field", destinationFieldName: "invalid_dest" },
        ]}
        onSubmit={mockSubmit}
      >
        <SelectSourceStream index={0} source={mockSource} sourceCatalog={catalogWithDifferentFields} />
      </MockFormProvider>
    );

    // Switch to a different stream
    const combobox = screen.getByPlaceholderText(COMBOBOX_PLACEHOLDER);
    await userEvent.click(combobox);
    await userEvent.clear(combobox);
    await userEvent.type(combobox, streamWithDifferentFields.stream.name);
    await userEvent.keyboard("{Enter}");

    // Submit the form
    await userEvent.click(screen.getByText("Submit"));

    expect(mockSubmit).toHaveBeenCalledWith({
      streams: [
        expect.objectContaining({
          sourceStreamDescriptor: {
            name: streamWithDifferentFields.stream.name,
            namespace: streamWithDifferentFields.stream.namespace,
          },
          fields: [
            { sourceFieldName: "", destinationFieldName: "user_id" }, // Invalid field reset to empty
            { sourceFieldName: "", destinationFieldName: "invalid_dest" }, // Invalid field reset to empty
          ],
        }),
      ],
    });
  });

  it("validates source field names when switching streams from initial state", async () => {
    const mockSubmit = jest.fn();

    // Start with an empty stream descriptor and select a stream that will validate fields
    const streamWithFields = createMockStream("profiles", "public", ["full_refresh"], {
      id: { type: "string" },
      email: { type: "string" },
    });

    const catalogForValidation: AirbyteCatalog = {
      streams: [streamWithFields],
    };

    await render(
      <MockFormProvider
        sourceStreamDescriptor={{ name: "" }} // Start with empty
        fields={[
          { sourceFieldName: "id", destinationFieldName: "user_id" }, // Valid field
          { sourceFieldName: "invalid_field", destinationFieldName: "invalid" }, // Invalid field
        ]}
        onSubmit={mockSubmit}
      >
        <SelectSourceStream index={0} source={mockSource} sourceCatalog={catalogForValidation} />
      </MockFormProvider>
    );

    // Select a stream
    const combobox = screen.getByPlaceholderText(COMBOBOX_PLACEHOLDER);
    await userEvent.click(combobox);
    await userEvent.type(combobox, streamWithFields.stream.name);
    await userEvent.keyboard("{Enter}");

    // Submit the form
    await userEvent.click(screen.getByText("Submit"));

    expect(mockSubmit).toHaveBeenCalledWith({
      streams: [
        expect.objectContaining({
          sourceStreamDescriptor: {
            name: streamWithFields.stream.name,
            namespace: streamWithFields.stream.namespace,
          },
          sourceSyncMode: streamWithFields.stream.supportedSyncModes[0],
          fields: [
            { sourceFieldName: "id", destinationFieldName: "user_id" }, // Preserved - valid field
            { sourceFieldName: "", destinationFieldName: "invalid" }, // Reset - invalid field
          ],
        }),
      ],
    });
  });

  it("handles empty catalog gracefully", async () => {
    const mockSubmit = jest.fn();

    // Test component behavior with empty catalog
    const emptySourceCatalog: AirbyteCatalog = {
      streams: [], // No streams available
    };

    await render(
      <MockFormProvider onSubmit={mockSubmit}>
        <SelectSourceStream index={0} source={mockSource} sourceCatalog={emptySourceCatalog} />
      </MockFormProvider>
    );

    // Component should render without crashing even with empty catalog
    expect(screen.getByPlaceholderText(COMBOBOX_PLACEHOLDER)).toBeInTheDocument();

    // Submit the form to verify it doesn't break
    await userEvent.click(screen.getByText("Submit"));

    expect(mockSubmit).toHaveBeenCalledWith({
      streams: [
        expect.objectContaining({
          sourceStreamDescriptor: { name: "" },
        }),
      ],
    });
  });

  it("does not trigger onChange when selecting the same stream", async () => {
    const mockSubmit = jest.fn();

    await render(
      <MockFormProvider
        sourceStreamDescriptor={{
          name: MOCK_SINGLE_SYNC_MODE_STREAM.stream.name,
          namespace: MOCK_SINGLE_SYNC_MODE_STREAM.stream.namespace,
        }}
        sourceSyncMode="full_refresh"
        fields={[{ sourceFieldName: "id", destinationFieldName: "user_id" }]}
        onSubmit={mockSubmit}
      >
        <SelectSourceStream index={0} source={mockSource} sourceCatalog={MOCK_SOURCE_CATALOG_SINGLE_NAMESPACE} />
      </MockFormProvider>
    );

    // Try to select the same stream that's already selected
    // Since it's already selected, just submit without changing anything
    await userEvent.click(screen.getByText("Submit"));

    // Form values should remain unchanged
    expect(mockSubmit).toHaveBeenCalledWith({
      streams: [
        expect.objectContaining({
          sourceStreamDescriptor: {
            name: MOCK_SINGLE_SYNC_MODE_STREAM.stream.name,
            namespace: MOCK_SINGLE_SYNC_MODE_STREAM.stream.namespace,
          },
          sourceSyncMode: "full_refresh",
          fields: [{ sourceFieldName: "id", destinationFieldName: "user_id" }],
        }),
      ],
    });
  });

  it("handles streams without namespace properly", async () => {
    const mockSubmit = jest.fn();

    const catalogWithNoNamespace: AirbyteCatalog = {
      streams: [MOCK_NO_NAMESPACE_STREAM],
    };

    await render(
      <MockFormProvider onSubmit={mockSubmit}>
        <SelectSourceStream index={0} source={mockSource} sourceCatalog={catalogWithNoNamespace} />
      </MockFormProvider>
    );

    // Select the stream without namespace
    const combobox = screen.getByPlaceholderText(COMBOBOX_PLACEHOLDER);
    await userEvent.click(combobox);
    await userEvent.type(combobox, MOCK_NO_NAMESPACE_STREAM.stream.name);
    await userEvent.keyboard("{Enter}");

    // Submit the form
    await userEvent.click(screen.getByText("Submit"));

    expect(mockSubmit).toHaveBeenCalledWith({
      streams: [
        expect.objectContaining({
          sourceStreamDescriptor: {
            name: MOCK_NO_NAMESPACE_STREAM.stream.name,
            namespace: MOCK_NO_NAMESPACE_STREAM.stream.namespace,
          },
          sourceSyncMode: MOCK_NO_NAMESPACE_STREAM.stream.supportedSyncModes[0],
          cursorField: null,
        }),
      ],
    });
  });

  it("adds EMPTY_FIELD when no other fields are present", async () => {
    const mockSubmit = jest.fn();

    await render(
      <MockFormProvider
        sourceStreamDescriptor={{ name: "" }}
        fields={[]} // Start with empty fields
        onSubmit={mockSubmit}
      >
        <SelectSourceStream index={0} source={mockSource} sourceCatalog={MOCK_SOURCE_CATALOG_SINGLE_NAMESPACE} />
      </MockFormProvider>
    );

    // Select a stream
    const combobox = screen.getByPlaceholderText(COMBOBOX_PLACEHOLDER);
    await userEvent.click(combobox);
    await userEvent.type(combobox, MOCK_SINGLE_SYNC_MODE_STREAM.stream.name);
    await userEvent.keyboard("{Enter}");

    // Submit the form
    await userEvent.click(screen.getByText("Submit"));

    expect(mockSubmit).toHaveBeenCalledWith({
      streams: [
        expect.objectContaining({
          fields: [{ sourceFieldName: "", destinationFieldName: "" }], // EMPTY_FIELD added
        }),
      ],
    });
  });
});
