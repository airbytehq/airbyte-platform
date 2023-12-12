import { renderHook } from "@testing-library/react";

import { mockConnection } from "test-utils/mock-data/mockConnection";

import { useStreamFilters } from "./useStreamFilters";
import { SyncStreamFieldWithId } from "../../ConnectionForm/formConfig";

describe("useStreamFilters", () => {
  it("should return the same list if the search string is empty and hideDisabledStreams is false", () => {
    const { result } = renderHook(() =>
      useStreamFilters("", false, mockConnection.syncCatalog.streams as SyncStreamFieldWithId[])
    );
    expect(result.current).toHaveLength(mockConnection.syncCatalog.streams.length);
  });
  it("should filter by stream name", () => {
    const { result } = renderHook(() =>
      useStreamFilters("pokemon", false, mockConnection.syncCatalog.streams as SyncStreamFieldWithId[])
    );
    expect(result.current).toHaveLength(2);
  });
  it("should filter by disabled streams", () => {
    const { result } = renderHook(() =>
      useStreamFilters("", true, mockConnection.syncCatalog.streams as SyncStreamFieldWithId[])
    );
    expect(result.current).toHaveLength(2);
  });
  it("should filter by both stream name and disabled streams", () => {
    const { result } = renderHook(() =>
      useStreamFilters("pokemon", true, mockConnection.syncCatalog.streams as SyncStreamFieldWithId[])
    );
    expect(result.current).toHaveLength(1);
  });
});
