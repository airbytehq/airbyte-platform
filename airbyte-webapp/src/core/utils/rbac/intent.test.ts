import { renderHook } from "@testing-library/react";

import { useIntent } from "./intent";
import { useRbac } from "./rbac";

const testIntents = {
  __Mock_OrganizationReader: { resourceType: "ORGANIZATION", role: "READER" },
  __Mock_WorkspaceReader: { resourceType: "WORKSPACE", role: "READER" },
} as const;
type TestIntents = typeof testIntents;
declare module "./intent" {
  // eslint-disable-next-line jest/no-export, @typescript-eslint/no-empty-interface
  export interface AllIntents extends TestIntents {}
}
jest.mock("./intents", () => ({ intentToRbacQuery: testIntents }));

jest.mock("./rbac", () => ({
  useRbac: jest.fn(),
}));
const mockUseRbac = useRbac as unknown as jest.Mock;

describe("useIntent", () => {
  it("maps intent to query", () => {
    mockUseRbac.mockClear();
    renderHook(() => useIntent("__Mock_OrganizationReader", { organizationId: undefined }));
    expect(mockUseRbac).toHaveBeenCalledTimes(1);
    expect(mockUseRbac).toHaveBeenCalledWith([
      {
        resourceType: "ORGANIZATION",
        role: "READER",
        resourceId: undefined,
      },
    ]);
  });

  describe("applies overriding details", () => {
    it("overrides the organizationId", () => {
      mockUseRbac.mockClear();
      renderHook(() => useIntent("__Mock_OrganizationReader", { organizationId: "some-other-org" }));
      expect(mockUseRbac).toHaveBeenCalledTimes(1);
      expect(mockUseRbac).toHaveBeenCalledWith([
        {
          resourceType: "ORGANIZATION",
          role: "READER",
          resourceId: "some-other-org",
        },
      ]);
    });

    it("overrides the workspaceId", () => {
      mockUseRbac.mockClear();
      renderHook(() => useIntent("__Mock_WorkspaceReader", { workspaceId: "some-other-workspace" }));
      expect(mockUseRbac).toHaveBeenCalledTimes(1);
      expect(mockUseRbac).toHaveBeenCalledWith([
        {
          resourceType: "WORKSPACE",
          role: "READER",
          resourceId: "some-other-workspace",
        },
      ]);
    });

    it("does not override a resourceId with that of a mismatched resource", () => {
      mockUseRbac.mockClear();
      renderHook(() =>
        // @ts-expect-error we're testing invalid object shapes
        useIntent("__Mock_OrganizationReader", { workspaceId: "some-other-organization" }, mockUseRbac)
      );
      expect(mockUseRbac).toHaveBeenCalledTimes(1);
      expect(mockUseRbac).toHaveBeenCalledWith([
        {
          resourceType: "ORGANIZATION",
          role: "READER",
        },
      ]);

      mockUseRbac.mockClear();
      // @ts-expect-error we're testing invalid object shapes
      renderHook(() => useIntent("__Mock_WorkspaceReader", { organizationId: "some-other-workspace" }, mockUseRbac));
      expect(mockUseRbac).toHaveBeenCalledTimes(1);
      expect(mockUseRbac).toHaveBeenCalledWith([
        {
          resourceType: "WORKSPACE",
          role: "READER",
        },
      ]);
    });
  });

  // eslint-disable-next-line jest/expect-expect
  it("intent meta property enforcement", () => {
    const processIntent = useIntent; // avoid react rules of hooks warnings 🤡

    // @TODO: if we have any instance-level intents, add checks here to exclude organizationId and workspaceId

    processIntent("__Mock_OrganizationReader", { organizationId: "org" });
    // @ts-expect-error workspaceId is not valid for ListOrganizationREADERs
    processIntent("__Mock_OrganizationReader", { workspaceId: "workspace" });

    processIntent("__Mock_WorkspaceReader", { workspaceId: "workspace" });
    // @ts-expect-error workspaceId is not valid for ListWorkspaceREADERs
    processIntent("__Mock_WorkspaceReader", { organizationId: "organizationId" });
  });
});
