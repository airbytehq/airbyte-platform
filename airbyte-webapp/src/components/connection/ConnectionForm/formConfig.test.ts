import { renderHook } from "@testing-library/react";
import cloneDeep from "lodash/cloneDeep";

import { mockConnection } from "test-utils/mock-data/mockConnection";
import {
  mockDestinationDefinitionSpecification,
  mockDestinationDefinitionVersion,
} from "test-utils/mock-data/mockDestination";
import { mockWorkspace } from "test-utils/mock-data/mockWorkspace";

import { useInitialFormValues } from "./formConfig";

jest.mock("core/api", () => ({
  useCurrentWorkspace: () => mockWorkspace,
}));

describe("#useInitialFormValues", () => {
  it("should generate initial values w/ mode: readonly", () => {
    const { result } = renderHook(() =>
      useInitialFormValues(
        mockConnection,
        mockDestinationDefinitionVersion,
        mockDestinationDefinitionSpecification,
        "readonly"
      )
    );
    expect(result.current).toMatchSnapshot();
    expect(result.current.name).toBeDefined();
  });

  it("should generate initial values w/ mode: create", () => {
    const { result } = renderHook(() =>
      useInitialFormValues(
        mockConnection,
        mockDestinationDefinitionVersion,
        mockDestinationDefinitionSpecification,
        "create"
      )
    );
    expect(result.current).toMatchSnapshot();
    expect(result.current.name).toBeDefined();
  });

  it("should generate initial values w/ mode: edit", () => {
    const { result } = renderHook(() =>
      useInitialFormValues(
        mockConnection,
        mockDestinationDefinitionVersion,
        mockDestinationDefinitionSpecification,
        "edit"
      )
    );
    expect(result.current).toMatchSnapshot();
    expect(result.current.name).toBeDefined();
  });

  it("should pick best sync mode in create mode", () => {
    const connection = cloneDeep(mockConnection);
    connection.syncCatalog.streams[0].stream!.supportedSyncModes = ["full_refresh", "incremental"];
    const { result } = renderHook(() =>
      useInitialFormValues(
        connection,
        mockDestinationDefinitionVersion,
        mockDestinationDefinitionSpecification,
        "create"
      )
    );
    expect(result.current.syncCatalog.streams[0].config?.syncMode).toBe("incremental");
    expect(result.current.syncCatalog.streams[0].config?.destinationSyncMode).toBe("append_dedup");
  });

  it("should not change sync mode in readonly mode", () => {
    const connection = cloneDeep(mockConnection);
    connection.syncCatalog.streams[0].stream!.supportedSyncModes = ["full_refresh", "incremental"];
    connection.syncCatalog.streams[0].config!.destinationSyncMode = "append";
    const { result } = renderHook(() =>
      useInitialFormValues(
        connection,
        mockDestinationDefinitionVersion,
        mockDestinationDefinitionSpecification,
        "readonly"
      )
    );
    expect(result.current.syncCatalog.streams[0].config?.syncMode).toBe("full_refresh");
    expect(result.current.syncCatalog.streams[0].config?.destinationSyncMode).toBe("append");
  });

  it("should not change sync mode in edit mode", () => {
    const connection = cloneDeep(mockConnection);
    connection.syncCatalog.streams[0].stream!.supportedSyncModes = ["full_refresh", "incremental"];
    connection.syncCatalog.streams[0].config!.destinationSyncMode = "append";

    const { result } = renderHook(() =>
      useInitialFormValues(connection, mockDestinationDefinitionVersion, mockDestinationDefinitionSpecification, "edit")
    );
    expect(result.current.syncCatalog.streams[0].config?.syncMode).toBe("full_refresh");
    expect(result.current.syncCatalog.streams[0].config?.destinationSyncMode).toBe("append");
  });

  // This is a low-priority test
  it.todo(
    "should test for supportsDbt+initialValues.transformations and supportsNormalization+initialValues.normalization"
  );
});
