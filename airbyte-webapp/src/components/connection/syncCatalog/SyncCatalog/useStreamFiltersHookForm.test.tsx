import { renderHook } from "@testing-library/react";

import { mockConnection } from "test-utils/mock-data/mockConnection";

import { useStreamFiltersHookForm } from "./useStreamFiltersHookForm";
import { SyncStreamFieldWithId } from "../../ConnectionForm/hookFormConfig";

describe("useStreamFiltersHookForm", () => {
  it("should return the same list if the search string is empty and hideDisabledStreams is false", () => {
    const { result } = renderHook(() =>
      useStreamFiltersHookForm("", false, mockConnection.syncCatalog.streams as SyncStreamFieldWithId[])
    );
    expect(result.current).toHaveLength(mockConnection.syncCatalog.streams.length);
  });
  it("should filter by stream name", () => {
    const { result } = renderHook(() =>
      useStreamFiltersHookForm("pokemon", false, mockConnection.syncCatalog.streams as SyncStreamFieldWithId[])
    );
    expect(result.current).toHaveLength(2);
  });
  it("should filter by disabled streams", () => {
    const { result } = renderHook(() =>
      useStreamFiltersHookForm("", true, mockConnection.syncCatalog.streams as SyncStreamFieldWithId[])
    );
    expect(result.current).toHaveLength(2);
  });
  it("should filter by both stream name and disabled streams", () => {
    const { result } = renderHook(() =>
      useStreamFiltersHookForm("pokemon", true, mockConnection.syncCatalog.streams as SyncStreamFieldWithId[])
    );
    expect(result.current).toHaveLength(1);
  });
});
