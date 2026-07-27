import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { ReactNode } from "react";

import { useInitialValidation, useValidateMappers } from "./mappers";
import { webBackendValidateMappers } from "../generated/AirbyteClient";
import {
  ConfiguredStreamMapper,
  HashingMapperConfigurationMethod,
  MapperValidationError,
  MapperValidationErrorType,
  StreamMapperType,
} from "../types/AirbyteClient";

jest.mock("../generated/AirbyteClient", () => ({
  webBackendValidateMappers: jest.fn(),
}));

jest.mock("../useRequestOptions", () => ({
  useRequestOptions: jest.fn(() => ({})),
}));

jest.mock("./connections", () => ({
  useCurrentConnection: jest.fn(() => ({ connectionId: "connection-id" })),
}));

jest.mock("../useSuspenseQuery", () => ({
  useSuspenseQuery: jest.fn((_queryKey, queryFn) => queryFn()),
}));

const mockWebBackendValidateMappers = webBackendValidateMappers as jest.MockedFunction<
  typeof webBackendValidateMappers
>;

describe("mapper validation request bodies", () => {
  let queryClient: QueryClient;

  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );

  const streamDescriptor = { name: "users", namespace: "public" };
  const mapper: ConfiguredStreamMapper & {
    validationError: MapperValidationError;
    validationCallback: () => Promise<boolean>;
  } = {
    id: "mapper-id",
    type: StreamMapperType.hashing,
    mapperConfiguration: {
      method: HashingMapperConfigurationMethod["SHA-256"],
      targetField: "email",
      fieldNameSuffix: "_hashed",
    },
    validationError: {
      type: MapperValidationErrorType.FIELD_NOT_FOUND,
      message: "UI-only validation state",
    },
    validationCallback: jest.fn(() => Promise.resolve(true)),
  };

  beforeEach(() => {
    jest.clearAllMocks();
    queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    mockWebBackendValidateMappers.mockResolvedValue({ initialFields: [], mappers: [], outputFields: [] });
  });

  afterEach(() => {
    queryClient.clear();
  });

  it("sends only declared mapper fields during live validation", async () => {
    const { result } = renderHook(() => useValidateMappers(), { wrapper });

    await result.current.fetchQuery(streamDescriptor, [mapper]);

    expect(mockWebBackendValidateMappers).toHaveBeenCalledWith(
      {
        connectionId: "connection-id",
        streamDescriptor,
        mappers: [
          {
            id: "mapper-id",
            type: StreamMapperType.hashing,
            mapperConfiguration: mapper.mapperConfiguration,
          },
        ],
      },
      {}
    );
  });

  it("sends only declared mapper fields during initial validation", async () => {
    renderHook(() => useInitialValidation(streamDescriptor, [mapper]), { wrapper });

    await waitFor(() =>
      expect(mockWebBackendValidateMappers).toHaveBeenCalledWith(
        {
          connectionId: "connection-id",
          streamDescriptor,
          mappers: [
            {
              id: "mapper-id",
              type: StreamMapperType.hashing,
              mapperConfiguration: mapper.mapperConfiguration,
            },
          ],
        },
        {}
      )
    );
  });
});
