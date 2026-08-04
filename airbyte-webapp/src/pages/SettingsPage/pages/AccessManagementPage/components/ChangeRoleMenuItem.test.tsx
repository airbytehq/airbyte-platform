import { PermissionType } from "core/api/types/AirbyteClient";

import { disallowedRoles } from "./ChangeRoleMenuItem";
import { UnifiedUserModel } from "./util";

const user = (organizationPermissionType?: PermissionType, workspacePermissionType?: PermissionType) =>
  ({
    id: "user1",
    organizationPermission: organizationPermissionType
      ? { permissionType: organizationPermissionType, permissionId: "org-permission", userId: "user1" }
      : undefined,
    workspacePermission: workspacePermissionType
      ? { permissionType: workspacePermissionType, permissionId: "ws-permission", userId: "user1" }
      : undefined,
  }) as UnifiedUserModel;

const ACTOR_SCOPED = [PermissionType.workspace_source_editor, PermissionType.workspace_destination_editor];

describe("disallowedRoles", () => {
  it("disallows the actor-scoped editors for the current user", () => {
    const result = disallowedRoles(user(), "workspace", true);
    ACTOR_SCOPED.forEach((role) => expect(result).toContain(role));
  });

  it("disallows the actor-scoped editors when an organization editor grants them already", () => {
    const result = disallowedRoles(user(PermissionType.organization_editor), "workspace", false);
    ACTOR_SCOPED.forEach((role) => expect(result).toContain(role));
  });

  it("disallows the actor-scoped editors for an organization admin", () => {
    const result = disallowedRoles(user(PermissionType.organization_admin), "workspace", false);
    ACTOR_SCOPED.forEach((role) => expect(result).toContain(role));
  });

  it("leaves the actor-scoped editors selectable for an organization runner", () => {
    const result = disallowedRoles(user(PermissionType.organization_runner), "workspace", false);
    ACTOR_SCOPED.forEach((role) => expect(result).not.toContain(role));
  });

  it("leaves the actor-scoped editors selectable for an organization reader", () => {
    const result = disallowedRoles(user(PermissionType.organization_reader), "workspace", false);
    ACTOR_SCOPED.forEach((role) => expect(result).not.toContain(role));
  });

  it("leaves the actor-scoped editors selectable for a user with no organization role", () => {
    const result = disallowedRoles(user(), "workspace", false);
    ACTOR_SCOPED.forEach((role) => expect(result).not.toContain(role));
  });

  it("disallows nothing at the organization scope", () => {
    expect(disallowedRoles(user(PermissionType.organization_admin), "organization", false)).toEqual([]);
  });
});
