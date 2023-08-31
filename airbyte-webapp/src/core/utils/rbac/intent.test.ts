import { renderHook } from "@testing-library/react";

import { Intent, useIntent } from "./intent";
import { useRbac } from "./rbac";

jest.mock("./rbac", () => ({
  useRbac: jest.fn(),
}));
const mockUseRbac = useRbac as unknown as jest.Mock;

describe("useIntent", () => {
  it("maps intent to query", () => {
    mockUseRbac.mockClear();
    renderHook(() => useIntent(Intent.ListOrganizationMembers, undefined));
    expect(mockUseRbac).toHaveBeenCalledTimes(1);
    expect(mockUseRbac).toHaveBeenCalledWith({
      resourceType: "ORGANIZATION",
      role: "READER",
    });
  });

  describe("applies overriding details", () => {
    it("overrides the organizationId", () => {
      mockUseRbac.mockClear();
      renderHook(() => useIntent(Intent.ListOrganizationMembers, { organizationId: "some-other-org" }));
      expect(mockUseRbac).toHaveBeenCalledTimes(1);
      expect(mockUseRbac).toHaveBeenCalledWith({
        resourceType: "ORGANIZATION",
        role: "READER",
        resourceId: "some-other-org",
      });
    });

    it("overrides the workspaceId", () => {
      mockUseRbac.mockClear();
      renderHook(() => useIntent(Intent.ListWorkspaceMembers, { workspaceId: "some-other-workspace" }));
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
        useIntent(Intent.ListOrganizationMembers, { workspaceId: "some-other-organization" }, mockUseRbac)
      );
      expect(mockUseRbac).toHaveBeenCalledTimes(1);
      expect(mockUseRbac).toHaveBeenCalledWith({
        resourceType: "ORGANIZATION",
        role: "READER",
      });

      mockUseRbac.mockClear();
      // @ts-expect-error we're testing invalid object shapes
      renderHook(() => useIntent(Intent.ListWorkspaceMembers, { organizationId: "some-other-workspace" }, mockUseRbac));
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

    processIntent(Intent.ListOrganizationMembers);
    processIntent(Intent.ListOrganizationMembers, { organizationId: "org" });
    // @ts-expect-error workspaceId is not valid for ListOrganizationMembers
    processIntent(Intent.ListOrganizationMembers, { workspaceId: "workspace" });

    processIntent(Intent.ListWorkspaceMembers);
    processIntent(Intent.ListWorkspaceMembers, { workspaceId: "workspace" });
    // @ts-expect-error workspaceId is not valid for ListWorkspaceMembers
    processIntent(Intent.ListWorkspaceMembers, { organizationId: "organizationId" });
  });
});
