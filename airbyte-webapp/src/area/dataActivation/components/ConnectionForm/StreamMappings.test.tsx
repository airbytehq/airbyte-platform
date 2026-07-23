import { screen } from "@testing-library/react";
import { userEvent } from "@testing-library/user-event";
import { PropsWithChildren } from "react";
import { FormProvider, useForm } from "react-hook-form";

import { mockDestination, mockSource, render } from "test-utils";

import { DataActivationConnectionFormValues } from "area/dataActivation/types";
import { AirbyteCatalog, DestinationCatalog, SyncMode } from "core/api/types/AirbyteClient";
import { AirbyteThemeProvider } from "core/utils/useAirbyteTheme";

import { StreamMappings } from "./StreamMappings";

const mockSourceCatalog: AirbyteCatalog = {
  streams: [
    {
      stream: {
        name: "users",
        namespace: "public",
        supportedSyncModes: ["full_refresh", "incremental"] as SyncMode[],
        jsonSchema: {
          type: "object",
          properties: {
            id: { type: "string" },
            email: { type: "string" },
          },
        },
      },
      config: {
        syncMode: "full_refresh",
        destinationSyncMode: "append",
        selected: true,
      },
    },
  ],
};

const mockDestinationCatalog: DestinationCatalog = {
  operations: [
    {
      objectName: "Contact",
      syncMode: "append",
      schema: {
        type: "object",
        additionalProperties: false,
        properties: {
          FirstName: { type: "string" },
          LastName: { type: "string" },
        },
      },
    },
  ],
};

interface MockFormProviderProps {
  onSubmit?: (values: DataActivationConnectionFormValues) => void;
}

const MockFormProvider: React.FC<PropsWithChildren<MockFormProviderProps>> = ({ children, onSubmit = jest.fn() }) => {
  const methods = useForm<DataActivationConnectionFormValues>({
    defaultValues: {
      streams: [
        {
          sourceStreamDescriptor: { name: "users", namespace: "public" },
          sourceSyncMode: "full_refresh",
          cursorField: null,
          destinationObjectName: "Contact",
          destinationSyncMode: "append",
          matchingKeys: null,
          fields: [{ sourceFieldName: "id", destinationFieldName: "FirstName" }],
        },
      ],
    },
  });

  return (
    <AirbyteThemeProvider>
      <FormProvider {...methods}>
        <form onSubmit={methods.handleSubmit(onSubmit)}>{children}</form>
      </FormProvider>
    </AirbyteThemeProvider>
  );
};

describe("StreamMappings", () => {
  it("renders source refresh button when onRefreshSourceSchema is provided", async () => {
    const mockRefreshSource = jest.fn();

    await render(
      <MockFormProvider>
        <StreamMappings
          source={mockSource}
          sourceCatalog={mockSourceCatalog}
          destination={mockDestination}
          destinationCatalog={mockDestinationCatalog}
          onRefreshSourceSchema={mockRefreshSource}
        />
      </MockFormProvider>
    );

    const refreshButtons = screen.getAllByTestId("refreshSchemaButton");
    expect(refreshButtons.length).toBeGreaterThan(0);
  });

  it("renders destination refresh button when onRefreshDestinationCatalog is provided", async () => {
    const mockRefreshDestination = jest.fn();

    await render(
      <MockFormProvider>
        <StreamMappings
          source={mockSource}
          sourceCatalog={mockSourceCatalog}
          destination={mockDestination}
          destinationCatalog={mockDestinationCatalog}
          onRefreshDestinationCatalog={mockRefreshDestination}
        />
      </MockFormProvider>
    );

    const refreshButtons = screen.getAllByTestId("refreshSchemaButton");
    expect(refreshButtons.length).toBeGreaterThan(0);
  });

  it("does not render source refresh button when onRefreshSourceSchema is not provided", async () => {
    const mockRefreshDestination = jest.fn();

    await render(
      <MockFormProvider>
        <StreamMappings
          source={mockSource}
          sourceCatalog={mockSourceCatalog}
          destination={mockDestination}
          destinationCatalog={mockDestinationCatalog}
          onRefreshDestinationCatalog={mockRefreshDestination}
        />
      </MockFormProvider>
    );

    // Should only have one refresh button (for destination)
    const refreshButtons = screen.getAllByTestId("refreshSchemaButton");
    expect(refreshButtons).toHaveLength(1);
  });

  it("calls onRefreshSourceSchema when source refresh button is clicked", async () => {
    const mockRefreshSource = jest.fn().mockResolvedValue(undefined);

    await render(
      <MockFormProvider>
        <StreamMappings
          source={mockSource}
          sourceCatalog={mockSourceCatalog}
          destination={mockDestination}
          destinationCatalog={mockDestinationCatalog}
          onRefreshSourceSchema={mockRefreshSource}
          onRefreshDestinationCatalog={jest.fn()}
        />
      </MockFormProvider>
    );

    const refreshButtons = screen.getAllByTestId("refreshSchemaButton");
    // First button should be source refresh
    await userEvent.click(refreshButtons[0]);

    expect(mockRefreshSource).toHaveBeenCalled();
  });

  it("calls onRefreshDestinationCatalog when destination refresh button is clicked", async () => {
    const mockRefreshDestination = jest.fn().mockResolvedValue(undefined);

    await render(
      <MockFormProvider>
        <StreamMappings
          source={mockSource}
          sourceCatalog={mockSourceCatalog}
          destination={mockDestination}
          destinationCatalog={mockDestinationCatalog}
          onRefreshSourceSchema={jest.fn()}
          onRefreshDestinationCatalog={mockRefreshDestination}
        />
      </MockFormProvider>
    );

    const refreshButtons = screen.getAllByTestId("refreshSchemaButton");
    // Second button should be destination refresh
    await userEvent.click(refreshButtons[1]);

    expect(mockRefreshDestination).toHaveBeenCalled();
  });

  it("disables refresh buttons when disabled prop is true", async () => {
    const mockRefreshSource = jest.fn();
    const mockRefreshDestination = jest.fn();

    await render(
      <MockFormProvider>
        <StreamMappings
          source={mockSource}
          sourceCatalog={mockSourceCatalog}
          destination={mockDestination}
          destinationCatalog={mockDestinationCatalog}
          onRefreshSourceSchema={mockRefreshSource}
          onRefreshDestinationCatalog={mockRefreshDestination}
          disabled
        />
      </MockFormProvider>
    );

    const refreshButtons = screen.getAllByTestId("refreshSchemaButton");
    refreshButtons.forEach((button) => {
      expect(button).toBeDisabled();
    });
  });
});
