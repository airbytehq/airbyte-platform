import { z } from "zod";

import { PublicPermissionType } from "core/api/types/AirbyteClient";

/**
 * `permissionsByResourceType` (`AccessManagementPage/components/util.tsx`) is annotated
 * `Record<ResourceType, PermissionType[]>` — the full 13-value `PermissionType`, not the 11-value
 * `PublicPermissionType` this modal writes through `createGroupPermission`. Deriving a narrower
 * list from it would need a `satisfies` clause, so this modal keeps its own independent lists
 * instead. They double as the role dropdown options in the modal (Phase 3) and as the scope guard
 * below.
 */
export const ORGANIZATION_PERMISSION_TYPES: readonly PublicPermissionType[] = [
  PublicPermissionType.organization_admin,
  PublicPermissionType.organization_editor,
  PublicPermissionType.organization_runner,
  PublicPermissionType.organization_reader,
  PublicPermissionType.organization_member,
];

export const WORKSPACE_PERMISSION_TYPES: readonly PublicPermissionType[] = [
  PublicPermissionType.workspace_admin,
  PublicPermissionType.workspace_editor,
  PublicPermissionType.workspace_source_editor,
  PublicPermissionType.workspace_destination_editor,
  PublicPermissionType.workspace_runner,
  PublicPermissionType.workspace_reader,
];

const isKnownOrganizationPermissionType = (value: string): boolean =>
  (ORGANIZATION_PERMISSION_TYPES as readonly string[]).includes(value);

const isKnownWorkspacePermissionType = (value: string): boolean =>
  (WORKSPACE_PERMISSION_TYPES as readonly string[]).includes(value);

/**
 * `GroupPermissionRead.permissionType` is the full `PermissionType` (13 values); this modal can
 * therefore read a row — `instance_admin` or `workspace_owner` — that it has no way to write
 * through `GroupPermissionCreate`'s narrower `PublicPermissionType`. `permissionType` here is typed
 * as `PublicPermissionType` for every row this modal itself can create or edit, but a prefilled row
 * carries whatever the server returned, so callers that read prefill data must not assume the
 * runtime value is always one of the 11.
 */
export interface PermissionRowValue {
  /** Present iff the row exists on the server. Absent for a row the user just added. */
  permissionId?: string;
  permissionType: PublicPermissionType;
  /** Workspace rows only. */
  workspaceId?: string;
}

export interface GroupPermissionsFormValues {
  /** Length 0 or 1: a group holds at most one organization permission. */
  organizationPermission: PermissionRowValue[];
  workspacePermissions: PermissionRowValue[];
}

/**
 * Loose on `permissionType` and `workspaceId` shape deliberately: a prefilled row can carry a
 * `permissionType` outside `PublicPermissionType` (see the interface doc above), and that row must
 * still parse so the form can render it, disabled, rather than crash. The `superRefine` below only
 * flags a *known* role from the wrong scope — an organization role in the workspace section or vice
 * versa — which the dropdowns in Phase 3 cannot themselves produce, but a prefilled row is built
 * outside the dropdown flow and this guards it. An unrecognized value never triggers the refine, so
 * it never blocks Save.
 */
const organizationPermissionRowSchema = z
  .object({
    permissionId: z.string().optional(),
    permissionType: z.string().min(1, "form.empty.error"),
    workspaceId: z.string().optional(),
  })
  .superRefine((row, ctx) => {
    if (isKnownWorkspacePermissionType(row.permissionType)) {
      ctx.addIssue({ code: z.ZodIssueCode.custom, path: ["permissionType"], message: "form.empty.error" });
    }
  });

const workspacePermissionRowSchema = z
  .object({
    permissionId: z.string().optional(),
    permissionType: z.string().min(1, "form.empty.error"),
    workspaceId: z.string().min(1, "form.empty.error"),
  })
  .superRefine((row, ctx) => {
    if (isKnownOrganizationPermissionType(row.permissionType)) {
      ctx.addIssue({ code: z.ZodIssueCode.custom, path: ["permissionType"], message: "form.empty.error" });
    }
  });

export const groupPermissionsFormSchema = z.object({
  organizationPermission: z.array(organizationPermissionRowSchema).max(1),
  workspacePermissions: z.array(workspacePermissionRowSchema),
}) as unknown as z.ZodSchema<GroupPermissionsFormValues>;
