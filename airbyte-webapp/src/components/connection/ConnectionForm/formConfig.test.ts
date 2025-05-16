import { renderHook } from "@testing-library/react";
import cloneDeep from "lodash/cloneDeep";

import { mockConnection } from "test-utils/mock-data/mockConnection";
import { mockGetDataplaneGroup } from "test-utils/mock-data/mockDataplaneGroups";
import { mockDestinationDefinitionSpecification } from "test-utils/mock-data/mockDestination";
import { mockWorkspace } from "test-utils/mock-data/mockWorkspace";

import { useInitialFormValues } from "./formConfig";

jest.mock("core/api", () => ({
  useCurrentWorkspace: () => mockWorkspace,
  useGetDestinationDefinitionSpecification: () => mockDestinationDefinitionSpecification,
  useGetDataplaneGroup: () => mockGetDataplaneGroup,
}));

describe("#useInitialFormValues", () => {
  it("should generate initial values w/ mode: readonly", () => {
    const { result } = renderHook(() => useInitialFormValues(mockConnection, "readonly"));
    expect(result.current).toMatchSnapshot();
    expect(result.current.name).toBeDefined();
  });

  it("should generate initial values w/ mode: create", () => {
    const { result } = renderHook(() => useInitialFormValues(mockConnection, "create"));
    expect(result.current).toMatchSnapshot();
    expect(result.current.name).toBeDefined();
  });

  it("should generate initial values w/ mode: edit", () => {
    const { result } = renderHook(() => useInitialFormValues(mockConnection, "edit"));
    expect(result.current).toMatchSnapshot();
    expect(result.current.name).toBeDefined();
  });

  it("should pick best sync mode in create mode", () => {
    const connection = cloneDeep(mockConnection);
    connection.syncCatalog.streams[0].stream!.supportedSyncModes = ["full_refresh", "incremental"];
    const { result } = renderHook(() => useInitialFormValues(connection, "create"));
    expect(result.current.syncCatalog.streams[0].config?.syncMode).toBe("incremental");
    expect(result.current.syncCatalog.streams[0].config?.destinationSyncMode).toBe("append_dedup");
  });

  it("should not change sync mode in readonly mode", () => {
    const connection = cloneDeep(mockConnection);
    connection.syncCatalog.streams[0].stream!.supportedSyncModes = ["full_refresh", "incremental"];
    connection.syncCatalog.streams[0].config!.destinationSyncMode = "append";
    const { result } = renderHook(() => useInitialFormValues(connection, "readonly"));
    expect(result.current.syncCatalog.streams[0].config?.syncMode).toBe("full_refresh");
    expect(result.current.syncCatalog.streams[0].config?.destinationSyncMode).toBe("append");
  });

  it("should not change sync mode in edit mode", () => {
    const connection = cloneDeep(mockConnection);
    connection.syncCatalog.streams[0].stream!.supportedSyncModes = ["full_refresh", "incremental"];
    connection.syncCatalog.streams[0].config!.destinationSyncMode = "append";

    const { result } = renderHook(() => useInitialFormValues(connection, "edit"));
    expect(result.current.syncCatalog.streams[0].config?.syncMode).toBe("full_refresh");
    expect(result.current.syncCatalog.streams[0].config?.destinationSyncMode).toBe("append");
  });
});
