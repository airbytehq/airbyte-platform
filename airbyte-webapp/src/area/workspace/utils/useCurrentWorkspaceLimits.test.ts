import { renderHook } from "@testing-library/react";

import { mocked } from "test-utils";
import { mockWorkspace } from "test-utils/mock-data/mockWorkspace";
import { mockExperiments } from "test-utils/mockExperiments";

import { useGetWorkspace } from "core/api";
import { WorkspaceLimits } from "core/api/types/AirbyteClient";

import { useCurrentWorkspaceLimits } from "./useCurrentWorkspaceLimits";

jest.mock("core/api", () => ({
  useGetWorkspace: jest.fn(() => mockWorkspace),
  useRequestOptions: () => ({ getAccessToken: jest.fn(), includeCredentials: false }),
}));

jest.mock("area/workspace/utils", () => ({
  useCurrentWorkspaceId: () => "mock-id",
}));

const okWorkspaceLimits: WorkspaceLimits = {
  activeConnections: { current: 5, max: 10 },
  sources: { current: 5, max: 10 },
  destinations: { current: 5, max: 10 },
};

const exceededWorkspaceLimits: WorkspaceLimits = {
  activeConnections: { current: 99, max: 10 },
  sources: { current: 99, max: 10 },
  destinations: { current: 99, max: 10 },
};

const metWorkspaceLimits: WorkspaceLimits = {
  activeConnections: { current: 10, max: 10 },
  sources: { current: 10, max: 10 },
  destinations: { current: 10, max: 10 },
};

describe("useGetWouseCurrentWorkspaceLimitsrkspaceLimits", () => {
  beforeEach(() => {
    mockExperiments({ productLimitsUI: true });
  });

  it("should return all limits as false if workspaceLimits is undefined", () => {
    mocked(useGetWorkspace).mockReturnValueOnce({
      ...mockWorkspace,
      workspaceLimits: undefined,
    });
    const { result } = renderHook(() => useCurrentWorkspaceLimits());

    expect(result.current.activeConnectionLimitReached).toBe(false);
    expect(result.current.sourceLimitReached).toBe(false);
    expect(result.current.destinationLimitReached).toBe(false);
    expect(result.current.limits).toBe(undefined);
  });

  it("should return all limits as false if they have not been exceeded", () => {
    mocked(useGetWorkspace).mockReturnValueOnce({
      ...mockWorkspace,
      workspaceLimits: okWorkspaceLimits,
    });
    const { result } = renderHook(() => useCurrentWorkspaceLimits());

    expect(result.current.activeConnectionLimitReached).toBe(false);
    expect(result.current.sourceLimitReached).toBe(false);
    expect(result.current.destinationLimitReached).toBe(false);
    expect(result.current.limits).toEqual(okWorkspaceLimits);
  });

  it("should return all limits as false if the experiment is off", () => {
    mockExperiments({ productLimitsUI: false });

    mocked(useGetWorkspace).mockReturnValueOnce({
      ...mockWorkspace,
      workspaceLimits: exceededWorkspaceLimits,
    });
    const { result } = renderHook(() => useCurrentWorkspaceLimits());

    expect(result.current.activeConnectionLimitReached).toBe(false);
    expect(result.current.sourceLimitReached).toBe(false);
    expect(result.current.destinationLimitReached).toBe(false);
    expect(result.current.limits).toBe(undefined);
  });

  it("should return all limits as true if they have been met", () => {
    mocked(useGetWorkspace).mockReturnValueOnce({
      ...mockWorkspace,
      workspaceLimits: metWorkspaceLimits,
    });
    const { result } = renderHook(() => useCurrentWorkspaceLimits());

    expect(result.current.activeConnectionLimitReached).toBe(true);
    expect(result.current.sourceLimitReached).toBe(true);
    expect(result.current.destinationLimitReached).toBe(true);
    expect(result.current.limits).toEqual(metWorkspaceLimits);
  });

  it("should return all limits as true if they have been exceeded", () => {
    mocked(useGetWorkspace).mockReturnValueOnce({
      ...mockWorkspace,
      workspaceLimits: exceededWorkspaceLimits,
    });
    const { result } = renderHook(() => useCurrentWorkspaceLimits());

    expect(result.current.activeConnectionLimitReached).toBe(true);
    expect(result.current.sourceLimitReached).toBe(true);
    expect(result.current.destinationLimitReached).toBe(true);
    expect(result.current.limits).toEqual(exceededWorkspaceLimits);
  });
});
