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
  "CreateOrEditSource" = "CreateOrEditSource",
  "CreateOrEditDestination" = "CreateOrEditDestination",
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
      // Mirrors the role sets generated from intents.yaml for the two actor-scoped intents.
      CreateOrEditSource: {
        name: "Create or edit source",
        description: "Create a source connector, or change the settings of an existing source",
        roles: [
          "organization_editor",
          "organization_admin",
          "workspace_source_editor",
          "workspace_editor",
          "workspace_admin",
          "instance_admin",
        ],
      },
      CreateOrEditDestination: {
        name: "Create or edit destination",
        description: "Create a destination connector, or change the settings of an existing destination",
        roles: [
          "organization_editor",
          "organization_admin",
          "workspace_destination_editor",
          "workspace_editor",
          "workspace_admin",
          "instance_admin",
        ],
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
  useFirstOrg: () => ({
    organizationId: MOCK_ORGANIZATION_UUID,
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

  describe("actor-scoped workspace editors", () => {
    // The workspace id is passed explicitly: useGeneratedIntent reads it from
    // area/workspace/utils#useCurrentWorkspaceId, which is not mocked in this file.
    const renderIntent = (intent: MockIntent) =>
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      renderHook(() => useGeneratedIntent(intent as any, { workspaceId: MOCK_WORKSPACE_UUID }), {
        wrapper: TestWrapper,
      }).result;

    const withWorkspacePermission = (permissionType: string) =>
      mocked(useListPermissions).mockReturnValue({
        permissions: [
          {
            workspaceId: MOCK_WORKSPACE_UUID,
            permissionId: uuidv4(),
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            permissionType: permissionType as any,
            userId: MOCK_USER_ID,
          },
        ],
      });

    it("lets a source editor edit sources but not destinations", () => {
      withWorkspacePermission("workspace_source_editor");

      expect(renderIntent(MockIntent.CreateOrEditSource).current).toBe(true);
      expect(renderIntent(MockIntent.CreateOrEditDestination).current).toBe(false);
    });

    it("lets a destination editor edit destinations but not sources", () => {
      withWorkspacePermission("workspace_destination_editor");

      expect(renderIntent(MockIntent.CreateOrEditDestination).current).toBe(true);
      expect(renderIntent(MockIntent.CreateOrEditSource).current).toBe(false);
    });

    it("lets a full workspace editor edit both", () => {
      withWorkspacePermission("workspace_editor");

      expect(renderIntent(MockIntent.CreateOrEditSource).current).toBe(true);
      expect(renderIntent(MockIntent.CreateOrEditDestination).current).toBe(true);
    });

    it("does not let a runner edit either", () => {
      withWorkspacePermission("workspace_runner");

      expect(renderIntent(MockIntent.CreateOrEditSource).current).toBe(false);
      expect(renderIntent(MockIntent.CreateOrEditDestination).current).toBe(false);
    });
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
