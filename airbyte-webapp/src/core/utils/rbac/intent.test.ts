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
    renderHook(() => useIntent("ViewOrganizationSettings", undefined));
    expect(mockUseRbac).toHaveBeenCalledTimes(1);
    expect(mockUseRbac).toHaveBeenCalledWith({
      resourceType: "ORGANIZATION",
      role: "MEMBER",
    });
  });

  describe("applies overriding details", () => {
    it("overrides the organizationId", () => {
      mockUseRbac.mockClear();
      renderHook(() => useIntent("ViewOrganizationSettings", { organizationId: "some-other-org" }));
      expect(mockUseRbac).toHaveBeenCalledTimes(1);
      expect(mockUseRbac).toHaveBeenCalledWith({
        resourceType: "ORGANIZATION",
        role: "MEMBER",
        resourceId: "some-other-org",
      });
    });

    it("overrides the workspaceId", () => {
      mockUseRbac.mockClear();
      renderHook(() => useIntent("ViewWorkspaceSettings", { workspaceId: "some-other-workspace" }));
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
        useIntent("ViewOrganizationSettings", { workspaceId: "some-other-organization" }, mockUseRbac)
      );
      expect(mockUseRbac).toHaveBeenCalledTimes(1);
      expect(mockUseRbac).toHaveBeenCalledWith({
        resourceType: "ORGANIZATION",
        role: "MEMBER",
      });

      mockUseRbac.mockClear();
      // @ts-expect-error we're testing invalid object shapes
      renderHook(() => useIntent("ViewWorkspaceSettings", { organizationId: "some-other-workspace" }, mockUseRbac));
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

    processIntent("ViewOrganizationSettings");
    processIntent("ViewOrganizationSettings", { organizationId: "org" });
    // @ts-expect-error workspaceId is not valid for ViewOrganizationSettings
    processIntent("ViewOrganizationSettings", { workspaceId: "workspace" });

    processIntent("ViewWorkspaceSettings");
    processIntent("ViewWorkspaceSettings", { workspaceId: "workspace" });
    // @ts-expect-error workspaceId is not valid for ViewWorkspaceSettings
    processIntent("ViewWorkspaceSettings", { organizationId: "organizationId" });
  });
});
