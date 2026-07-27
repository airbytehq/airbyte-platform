import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook } from "@testing-library/react";
import { ReactNode } from "react";

import { useCreateConnection, useUpdateConnection } from "./connections";
import { webBackendCreateConnection, webBackendUpdateConnection } from "../generated/AirbyteClient";
import { ConnectionScheduleType, DestinationRead, NamespaceDefinitionType, SourceRead } from "../types/AirbyteClient";

jest.mock("../generated/AirbyteClient", () => ({
  webBackendCreateConnection: jest.fn(),
  webBackendUpdateConnection: jest.fn(),
}));

jest.mock("../useRequestOptions", () => ({
  useRequestOptions: jest.fn(() => ({})),
}));

jest.mock("./workspaces", () => ({
  useCurrentWorkspace: jest.fn(),
  useInvalidateWorkspaceStateQuery: jest.fn(() => jest.fn()),
}));

jest.mock("area/workspace/utils", () => ({
  useCurrentWorkspaceId: jest.fn(() => "workspace-id"),
}));

jest.mock("core/services/analytics", () => ({
  Action: { CREATE: "create", CREATE_FAILURE: "create_failure" },
  Namespace: { CONNECTION: "connection" },
  getFrequencyFromScheduleData: jest.fn(),
  useAnalyticsService: jest.fn(() => ({ track: jest.fn() })),
}));

jest.mock("core/services/Notification", () => ({
  useNotificationService: jest.fn(() => ({ registerNotification: jest.fn() })),
}));

jest.mock("core/errors", () => ({
  useFormatError: jest.fn(() => jest.fn()),
}));

jest.mock("react-router-dom", () => ({
  useNavigate: jest.fn(() => jest.fn()),
}));

jest.mock("react-intl", () => ({
  FormattedMessage: jest.fn(() => null),
  useIntl: jest.fn(() => ({ formatMessage: jest.fn() })),
}));

const mockWebBackendCreateConnection = webBackendCreateConnection as jest.MockedFunction<
  typeof webBackendCreateConnection
>;
const mockWebBackendUpdateConnection = webBackendUpdateConnection as jest.MockedFunction<
  typeof webBackendUpdateConnection
>;

describe("connection request bodies", () => {
  let queryClient: QueryClient;

  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );

  beforeEach(() => {
    jest.clearAllMocks();
    queryClient = new QueryClient({ defaultOptions: { mutations: { retry: false } } });
    mockWebBackendCreateConnection.mockResolvedValue({ connectionId: "connection-id" } as never);
    mockWebBackendUpdateConnection.mockResolvedValue({ connectionId: "connection-id" } as never);
  });

  afterEach(() => {
    queryClient.clear();
  });

  it.each([
    ["normal connection creation", false],
    ["data activation connection creation", true],
  ])("omits geography during %s", async (_name, isDataActivationConnection) => {
    const { result } = renderHook(() => useCreateConnection(), { wrapper });
    const values = {
      geography: "auto",
      namespaceDefinition: NamespaceDefinitionType.destination,
      scheduleType: ConnectionScheduleType.manual,
      syncCatalog: { streams: [] },
    };

    await result.current.mutateAsync({
      values,
      source: { sourceId: "source-id" } as SourceRead,
      destination: { destinationId: "destination-id" } as DestinationRead,
      isDataActivationConnection,
    });

    expect(mockWebBackendCreateConnection).toHaveBeenCalledWith(
      {
        namespaceDefinition: NamespaceDefinitionType.destination,
        scheduleType: ConnectionScheduleType.manual,
        syncCatalog: { streams: [] },
        sourceId: "source-id",
        destinationId: "destination-id",
        sourceCatalogId: undefined,
        destinationCatalogId: undefined,
        status: "active",
      },
      {}
    );
  });

  it("omits geography during a connection settings update", async () => {
    const { result } = renderHook(() => useUpdateConnection(), { wrapper });
    const update = {
      connectionId: "connection-id",
      geography: "auto",
      name: "updated connection",
      skipReset: true,
    };

    await result.current.mutateAsync(update);

    expect(mockWebBackendUpdateConnection).toHaveBeenCalledWith(
      {
        connectionId: "connection-id",
        name: "updated connection",
        skipReset: true,
      },
      {}
    );
  });
});
