import { renderHook } from "@testing-library/react";

import { useListPermissions } from "core/api";
import { PermissionRead } from "core/api/types/AirbyteClient";
import { useCurrentUser } from "core/services/auth";

import { useIsInstanceAdmin } from "./useIsInstanceAdmin";

jest.mock("core/api");
jest.mock("core/services/auth");

const mockUseListPermissions = useListPermissions as jest.MockedFunction<typeof useListPermissions>;
const mockUseCurrentUser = useCurrentUser as jest.MockedFunction<typeof useCurrentUser>;

const userId = "user-1";
const organizationId = "org-1";

const permission = (overrides: Partial<PermissionRead>): PermissionRead => ({
  permissionId: "permission-id",
  permissionType: "organization_member",
  userId,
  ...overrides,
});

const mockPermissions = (permissions: PermissionRead[]) => {
  mockUseListPermissions.mockReturnValue({ permissions });
};

beforeEach(() => {
  jest.clearAllMocks();
  mockUseCurrentUser.mockReturnValue({ userId } as ReturnType<typeof useCurrentUser>);
});

describe("useIsInstanceAdmin", () => {
  it("returns true when user is instance_admin", () => {
    mockPermissions([permission({ permissionType: "instance_admin" })]);

    const { result } = renderHook(() => useIsInstanceAdmin());

    expect(result.current).toBe(true);
  });

  it("returns false when user is organization_admin", () => {
    mockPermissions([permission({ permissionType: "organization_admin", organizationId })]);

    const { result } = renderHook(() => useIsInstanceAdmin());

    expect(result.current).toBe(false);
  });

  it.each(["organization_editor", "organization_runner", "organization_reader", "organization_member"] as const)(
    "returns false when user is %s",
    (permissionType) => {
      mockPermissions([permission({ permissionType, organizationId })]);

      const { result } = renderHook(() => useIsInstanceAdmin());

      expect(result.current).toBe(false);
    }
  );

  it("returns false when permissions list is empty", () => {
    mockPermissions([]);

    const { result } = renderHook(() => useIsInstanceAdmin());

    expect(result.current).toBe(false);
  });
});
