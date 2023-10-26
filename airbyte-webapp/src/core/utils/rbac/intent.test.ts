import { renderHook } from "@testing-library/react";

import { useIntent } from "./intent";
import { useRbac } from "./rbac";

jest.mock("./rbac", () => ({
  useRbac: jest.fn(),
}));
const mockUseRbac = useRbac as unknown as jest.Mock;

describe("useIntent", () => {
  it("maps intent to query", () => {
    mockUseRbac.mockClear();
    renderHook(() => useIntent("ListOrganizationMembers", undefined));
    expect(mockUseRbac).toHaveBeenCalledTimes(1);
    expect(mockUseRbac).toHaveBeenCalledWith({
      resourceType: "ORGANIZATION",
      role: "READER",
    });
  });

  describe("applies overriding details", () => {
    it("overrides the organizationId", () => {
      mockUseRbac.mockClear();
      renderHook(() => useIntent("ListOrganizationMembers", { organizationId: "some-other-org" }));
      expect(mockUseRbac).toHaveBeenCalledTimes(1);
      expect(mockUseRbac).toHaveBeenCalledWith({
        resourceType: "ORGANIZATION",
        role: "READER",
        resourceId: "some-other-org",
      });
    });

    it("overrides the workspaceId", () => {
      mockUseRbac.mockClear();
      renderHook(() => useIntent("ListWorkspaceMembers", { workspaceId: "some-other-workspace" }));
      expect(mockUseRbac).toHaveBeenCalledTimes(1);
      expect(mockUseRbac).toHaveBeenCalledWith({
        resourceType: "WORKSPACE",
        role: "READER",
        resourceId: "some-other-workspace",
      });
    });

    it("does not override a resourceId with that of a mismatched resource", () => {
      mockUseRbac.mockClear();
      renderHook(() =>
        // @ts-expect-error we're testing invalid object shapes
        useIntent("ListOrganizationMembers", { workspaceId: "some-other-organization" }, mockUseRbac)
      );
      expect(mockUseRbac).toHaveBeenCalledTimes(1);
      expect(mockUseRbac).toHaveBeenCalledWith({
        resourceType: "ORGANIZATION",
        role: "READER",
      });

      mockUseRbac.mockClear();
      // @ts-expect-error we're testing invalid object shapes
      renderHook(() => useIntent("ListWorkspaceMembers", { organizationId: "some-other-workspace" }, mockUseRbac));
      expect(mockUseRbac).toHaveBeenCalledTimes(1);
      expect(mockUseRbac).toHaveBeenCalledWith({
        resourceType: "WORKSPACE",
        role: "READER",
      });
    });
  });

  // eslint-disable-next-line jest/expect-expect
  it("intent meta property enforcement", () => {
    const processIntent = useIntent; // avoid react rules of hooks warnings 🤡

    // @TODO: if we have any instance-level intents, add checks here to exclude organizationId and workspaceId

    processIntent("ListOrganizationMembers");
    processIntent("ListOrganizationMembers", { organizationId: "org" });
    // @ts-expect-error workspaceId is not valid for ListOrganizationMembers
    processIntent("ListOrganizationMembers", { workspaceId: "workspace" });

    processIntent("ListWorkspaceMembers");
    processIntent("ListWorkspaceMembers", { workspaceId: "workspace" });
    // @ts-expect-error workspaceId is not valid for ListWorkspaceMembers
    processIntent("ListWorkspaceMembers", { organizationId: "organizationId" });
  });
});
