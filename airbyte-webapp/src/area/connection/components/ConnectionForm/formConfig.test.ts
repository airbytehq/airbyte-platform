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

  it("should set includeFiles to true for file-based streams when destination supports file transfer", () => {
    const connection = cloneDeep(mockConnection);
    // Make first stream file-based and selected
    connection.syncCatalog.streams[0].stream!.isFileBased = true;
    connection.syncCatalog.streams[0].config!.selected = true;
    connection.syncCatalog.streams[0].config!.includeFiles = false;
    // Make second stream file-based and unselected
    connection.syncCatalog.streams[1].stream!.isFileBased = true;
    connection.syncCatalog.streams[1].config!.selected = false;
    connection.syncCatalog.streams[1].config!.includeFiles = false;

    const { result } = renderHook(() => useInitialFormValues(connection, "edit", true));

    // File-based streams should remain unchanged when destination supports file transfer
    expect(result.current.syncCatalog.streams[0].config?.selected).toBe(true);
    expect(result.current.syncCatalog.streams[1].config?.selected).toBe(false);
    // includeFiles should be set to true for file-based streams
    expect(result.current.syncCatalog.streams[0].config?.includeFiles).toBe(true);
    expect(result.current.syncCatalog.streams[1].config?.includeFiles).toBe(true);
  });

  it("should disable file-based streams and set includeFiles to false when destination does not support file transfer", () => {
    const connection = cloneDeep(mockConnection);
    // Make first stream file-based and selected
    connection.syncCatalog.streams[0].stream!.isFileBased = true;
    connection.syncCatalog.streams[0].config!.selected = true;
    connection.syncCatalog.streams[0].config!.includeFiles = true;
    // Make second stream file-based and unselected
    connection.syncCatalog.streams[1].stream!.isFileBased = true;
    connection.syncCatalog.streams[1].config!.selected = false;
    connection.syncCatalog.streams[1].config!.includeFiles = true;
    // Keep third stream as non-file-based and selected
    connection.syncCatalog.streams[2].config!.selected = true;

    const { result } = renderHook(() => useInitialFormValues(connection, "edit", false));

    // File-based streams should be disabled when destination doesn't support file transfer
    expect(result.current.syncCatalog.streams[0].config?.selected).toBe(false);
    expect(result.current.syncCatalog.streams[1].config?.selected).toBe(false);
    // includeFiles should be set to false for file-based streams
    expect(result.current.syncCatalog.streams[0].config?.includeFiles).toBe(false);
    expect(result.current.syncCatalog.streams[1].config?.includeFiles).toBe(false);
    // Non-file-based streams should remain unchanged
    expect(result.current.syncCatalog.streams[2].config?.selected).toBe(true);
  });

  it("should only modify file-based streams and leave non-file-based streams unchanged", () => {
    const connection = cloneDeep(mockConnection);
    // Make first stream file-based and selected
    connection.syncCatalog.streams[0].stream!.isFileBased = true;
    connection.syncCatalog.streams[0].config!.selected = true;
    connection.syncCatalog.streams[0].config!.includeFiles = false;
    // Make second stream non-file-based and selected
    connection.syncCatalog.streams[1].stream!.isFileBased = false;
    connection.syncCatalog.streams[1].config!.selected = true;
    connection.syncCatalog.streams[1].config!.includeFiles = false;

    const { result } = renderHook(() => useInitialFormValues(connection, "edit", true));

    // File-based stream should have includeFiles set to true
    expect(result.current.syncCatalog.streams[0].config?.includeFiles).toBe(true);
    // Non-file-based stream should remain unchanged
    expect(result.current.syncCatalog.streams[1].config?.includeFiles).toBe(false);
  });

  it("should not modify any streams when destinationSupportsFileTransfer is undefined", () => {
    const connection = cloneDeep(mockConnection);
    // Make first stream file-based and selected
    connection.syncCatalog.streams[0].stream!.isFileBased = true;
    connection.syncCatalog.streams[0].config!.selected = true;
    connection.syncCatalog.streams[0].config!.includeFiles = false;

    const { result } = renderHook(() => useInitialFormValues(connection, "edit"));

    // When destinationSupportsFileTransfer is undefined, file-based streams should remain unchanged
    expect(result.current.syncCatalog.streams[0].config?.selected).toBe(true);
    expect(result.current.syncCatalog.streams[0].config?.includeFiles).toBe(false);
  });
});
