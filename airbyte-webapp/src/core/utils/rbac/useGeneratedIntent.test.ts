import { renderHook } from "@testing-library/react";
import { v4 as uuidv4 } from "uuid";

import { TestWrapper, mocked } from "test-utils";

import { useListPermissions } from "core/api";

import { useGeneratedIntent } from "./useGeneratedIntent";

const MOCK_USER_ID = uuidv4();
const MOCK_WORKSPACE_UUID = uuidv4();
const MOCK_ORGANIZATION_UUID = uuidv4();

enum MockIntent {
  "UploadCustomConnector" = "UploadCustomConnector",
}

jest.mock(
  "./generated-intents",
  () => ({
    INTENTS: {
      UploadCustomConnector: {
        name: "Create Custom Docker Connector",
        description: "Upload a custom docker connector to be used in the workspace",
        roles: ["organization_editor", "organization_admin", "workspace_editor", "workspace_admin", "instance_admin"],
      },
    },
  }),
  { virtual: true }
);

jest.mock("core/api", () => ({
  useListPermissions: jest.fn().mockResolvedValue({
    permissions: [],
  }),
  useCurrentWorkspaceOrUndefined: () => ({
    workspaceId: MOCK_WORKSPACE_UUID,
    organizationId: MOCK_ORGANIZATION_UUID,
  }),
  useGetDefaultUser: () => ({
    userId: MOCK_USER_ID,
  }),
}));

jest.mock("core/services/auth", () => ({
  useCurrentUser: () => ({
    userId: MOCK_USER_ID,
  }),
}));

describe(`${useGeneratedIntent.name}`, () => {
  it("returns true for instance_admin", () => {
    mocked(useListPermissions).mockReturnValue({
      permissions: [{ permissionId: uuidv4(), permissionType: "instance_admin", userId: MOCK_USER_ID }],
    });
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const { result } = renderHook(() => useGeneratedIntent(MockIntent.UploadCustomConnector as any), {
      wrapper: TestWrapper,
    });

    expect(result.current).toBe(true);
  });

  it("returns false if user has no permissions", () => {
    mocked(useListPermissions).mockReturnValue({
      permissions: [],
    });
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const { result } = renderHook(() => useGeneratedIntent(MockIntent.UploadCustomConnector as any), {
      wrapper: TestWrapper,
    });

    expect(result.current).toBe(false);
  });

  it("returns true if user has an organization admin permission from a matching org", () => {
    mocked(useListPermissions).mockReturnValue({
      permissions: [
        {
          organizationId: MOCK_ORGANIZATION_UUID,
          permissionId: uuidv4(),
          permissionType: "organization_admin",
          userId: MOCK_USER_ID,
        },
      ],
    });
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const { result } = renderHook(() => useGeneratedIntent(MockIntent.UploadCustomConnector as any), {
      wrapper: TestWrapper,
    });

    expect(result.current).toBe(true);
  });

  it("returns false if user has an organization admin permission from a different org", () => {
    const MOCK_SECOND_ORGANIZATION_UUID = uuidv4();
    mocked(useListPermissions).mockReturnValue({
      permissions: [
        {
          organizationId: MOCK_SECOND_ORGANIZATION_UUID,
          permissionId: uuidv4(),
          permissionType: "organization_admin",
          userId: MOCK_USER_ID,
        },
      ],
    });
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const { result } = renderHook(() => useGeneratedIntent(MockIntent.UploadCustomConnector as any), {
      wrapper: TestWrapper,
    });

    expect(result.current).toBe(false);
  });
});
