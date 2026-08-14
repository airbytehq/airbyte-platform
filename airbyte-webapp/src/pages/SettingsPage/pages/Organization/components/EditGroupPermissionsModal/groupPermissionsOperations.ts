import { GroupPermissionCreate, GroupPermissionRead, PublicPermissionType } from "core/api/types/AirbyteClient";

import { GroupPermissionsFormValues, PermissionRowValue } from "./groupPermissionsSchema";

type SectionPath = "organizationPermission" | "workspacePermissions";
type Scope = "organization" | "workspace";

export type Operation =
  | { kind: "create"; rowPath: string; body: GroupPermissionCreate }
  | { kind: "delete"; rowPath: string; permissionId: string }
  | {
      kind: "replace";
      rowPath: string;
      permissionId: string;
      body: GroupPermissionCreate;
      restore: GroupPermissionCreate;
    };

export type RowOutcome =
  | { status: "applied"; permissionId?: string }
  | { status: "createFailed" }
  | { status: "deleteFailed" }
  | { status: "rolledBack"; permissionId: string }
  | { status: "rollbackFailed" };

export interface OperationResult {
  operation: Operation;
  outcome: RowOutcome;
}

export interface OperationContext {
  groupId: string;
  organizationId: string;
}

/**
 * Exported so Phase 3 can render, but disable the controls of, a prefilled row this modal cannot
 * itself write (the "known edge case": a `permissionType` outside `PublicPermissionType`).
 */
export const isPublicPermissionType = (value: string): value is PublicPermissionType =>
  (Object.values(PublicPermissionType) as string[]).includes(value);

const buildCreateBody = (context: OperationContext, scope: Scope, row: PermissionRowValue): GroupPermissionCreate =>
  scope === "organization"
    ? { groupId: context.groupId, permissionType: row.permissionType, organizationId: context.organizationId }
    : { groupId: context.groupId, permissionType: row.permissionType, workspaceId: row.workspaceId };

const rowChanged = (a: PermissionRowValue, b: PermissionRowValue): boolean =>
  a.permissionType !== b.permissionType || a.workspaceId !== b.workspaceId;

const diffSection = (
  context: OperationContext,
  sectionPath: SectionPath,
  scope: Scope,
  baselineRows: PermissionRowValue[],
  valueRows: PermissionRowValue[]
): Operation[] => {
  const operations: Operation[] = [];
  const valuePermissionIds = new Set(valueRows.map((row) => row.permissionId).filter((id): id is string => !!id));

  // Rows the user added (no `permissionId`) are candidate creates, and baseline rows no longer
  // present in `values` are candidate deletes. Both are collected instead of pushed immediately,
  // because a `remove` followed by an `append` on the same scope — the UI's only way to change a
  // row's role — produces exactly this pair, and it must run as one `replace`, not as two
  // independent, concurrently executed operations (see the coalescing pass below).
  const pendingCreates: Array<{ rowPath: string; row: PermissionRowValue }> = [];

  valueRows.forEach((row, index) => {
    const rowPath = `${sectionPath}.${index}`;

    if (!row.permissionId) {
      pendingCreates.push({ rowPath, row });
      return;
    }

    const baselineRow = baselineRows.find((candidate) => candidate.permissionId === row.permissionId);
    if (!baselineRow || !isPublicPermissionType(baselineRow.permissionType)) {
      // Not on the baseline (shouldn't happen), or the "known edge case" row this modal can read
      // but never write. Either way, leave it alone.
      return;
    }

    if (!rowChanged(row, baselineRow)) {
      return;
    }

    operations.push({
      kind: "replace",
      rowPath,
      permissionId: row.permissionId,
      body: buildCreateBody(context, scope, row),
      restore: buildCreateBody(context, scope, baselineRow),
    });
  });

  const pendingDeletes: Array<{ rowPath: string; baselineRow: PermissionRowValue }> = [];

  baselineRows.forEach((baselineRow, index) => {
    if (!baselineRow.permissionId || valuePermissionIds.has(baselineRow.permissionId)) {
      return;
    }
    if (!isPublicPermissionType(baselineRow.permissionType)) {
      // The known edge case: never delete a row this modal cannot itself have written.
      return;
    }

    pendingDeletes.push({ rowPath: `${sectionPath}.${index}`, baselineRow });
  });

  // Coalesce a pending delete and a pending create that target the same scope into a single
  // `replace`. The organization section holds at most one row, so any pair coalesces; the
  // workspace section coalesces only a delete and a create for the *same* workspace — a delete of
  // workspace W and a create of a different workspace X stay independent.
  pendingCreates.forEach(({ rowPath, row }) => {
    const matchIndex = pendingDeletes.findIndex(({ baselineRow }) =>
      scope === "organization" ? true : baselineRow.workspaceId === row.workspaceId
    );

    if (matchIndex === -1) {
      operations.push({ kind: "create", rowPath, body: buildCreateBody(context, scope, row) });
      return;
    }

    const [{ baselineRow }] = pendingDeletes.splice(matchIndex, 1);

    if (row.permissionType === baselineRow.permissionType) {
      // The user removed a row and re-added the same role on the same scope: the end state
      // already equals the baseline, so this is a no-op, not a pointless delete-then-create.
      return;
    }

    operations.push({
      kind: "replace",
      rowPath,
      permissionId: baselineRow.permissionId!,
      body: buildCreateBody(context, scope, row),
      restore: buildCreateBody(context, scope, baselineRow),
    });
  });

  pendingDeletes.forEach(({ rowPath, baselineRow }) => {
    operations.push({ kind: "delete", rowPath, permissionId: baselineRow.permissionId! });
  });

  return operations;
};

/**
 * Classifies every row into at most one operation. Called against a mutable baseline that
 * successful operations advance (`applyOperationResults` below), so a retry after a partial
 * failure only reissues the operations that did not apply (D7).
 */
export const computeOperations = (
  context: OperationContext,
  baseline: GroupPermissionsFormValues,
  values: GroupPermissionsFormValues
): Operation[] => [
  ...diffSection(
    context,
    "organizationPermission",
    "organization",
    baseline.organizationPermission,
    values.organizationPermission
  ),
  ...diffSection(
    context,
    "workspacePermissions",
    "workspace",
    baseline.workspacePermissions,
    values.workspacePermissions
  ),
];

export interface GroupPermissionMutations {
  createGroupPermission: (body: GroupPermissionCreate) => Promise<GroupPermissionRead>;
  deleteGroupPermission: (permissionId: string) => Promise<void>;
}

const executeOperation = async (operation: Operation, mutations: GroupPermissionMutations): Promise<RowOutcome> => {
  if (operation.kind === "create") {
    try {
      const created = await mutations.createGroupPermission(operation.body);
      return { status: "applied", permissionId: created.permissionId };
    } catch {
      return { status: "createFailed" };
    }
  }

  if (operation.kind === "delete") {
    try {
      await mutations.deleteGroupPermission(operation.permissionId);
      return { status: "applied" };
    } catch {
      return { status: "deleteFailed" };
    }
  }

  // `replace`: delete, then create, then restore the original if the create half fails. The
  // three calls are sequential and dependent — a failed delete must suppress its paired create, or
  // a failed role change creates the second organization permission the ticket forbids (D1).
  try {
    await mutations.deleteGroupPermission(operation.permissionId);
  } catch {
    return { status: "deleteFailed" };
  }

  try {
    const created = await mutations.createGroupPermission(operation.body);
    return { status: "applied", permissionId: created.permissionId };
  } catch {
    try {
      const restored = await mutations.createGroupPermission(operation.restore);
      return { status: "rolledBack", permissionId: restored.permissionId };
    } catch {
      return { status: "rollbackFailed" };
    }
  }
};

/**
 * Rows are independent, so every operation runs concurrently and one row failing does not prevent
 * the others from applying.
 */
export const executeOperations = async (
  operations: Operation[],
  mutations: GroupPermissionMutations
): Promise<OperationResult[]> => {
  const settled = await Promise.allSettled(operations.map((operation) => executeOperation(operation, mutations)));

  return settled.map((result, index) => {
    const operation = operations[index];
    if (result.status === "fulfilled") {
      return { operation, outcome: result.value };
    }
    // executeOperation catches every rejection it can act on; a settled rejection here means an
    // unexpected throw. Fail closed with the most conservative outcome for the operation kind.
    return { operation, outcome: { status: operation.kind === "create" ? "createFailed" : "deleteFailed" } };
  });
};

const ROW_ERROR_MESSAGE_IDS: Record<Exclude<RowOutcome["status"], "applied">, string> = {
  createFailed: "settings.organization.groups.editPermissions.row.createFailed",
  deleteFailed: "settings.organization.groups.editPermissions.row.deleteFailed",
  rolledBack: "settings.organization.groups.editPermissions.row.rolledBack",
  rollbackFailed: "settings.organization.groups.editPermissions.row.rollbackFailed",
};

export interface RowError {
  rowPath: string;
  messageId: string;
}

export interface ApplyResultsOutcome {
  baseline: GroupPermissionsFormValues;
  values: GroupPermissionsFormValues;
  errors: RowError[];
  hasFailure: boolean;
}

const sectionOf = (values: GroupPermissionsFormValues, sectionPath: SectionPath): PermissionRowValue[] =>
  sectionPath === "organizationPermission" ? values.organizationPermission : values.workspacePermissions;

const withSection = (
  values: GroupPermissionsFormValues,
  sectionPath: SectionPath,
  rows: PermissionRowValue[]
): GroupPermissionsFormValues =>
  sectionPath === "organizationPermission"
    ? { ...values, organizationPermission: rows }
    : { ...values, workspacePermissions: rows };

const parseRowPath = (rowPath: string): { sectionPath: SectionPath; index: number } => {
  const [sectionPath, indexString] = rowPath.split(".");
  return { sectionPath: sectionPath as SectionPath, index: Number(indexString) };
};

/**
 * Applies every `OperationResult` to the baseline and to the submitted form values, per D8:
 *
 * - A successful create or replace sets the row's `permissionId` to the id the server returned.
 * - A successful plain delete needs no fixup: the row was already absent from `values`.
 * - A `rolledBack` row (delete succeeded, create failed, restore succeeded) resets to the previous
 *   role — the restore recreated the original permission under a new id — matching the "the
 *   previous role is still in place" copy.
 * - A `rollbackFailed` row (delete succeeded, create and restore both failed) clears its
 *   `permissionId`. The group now has no permission there; the row keeps the role the user was
 *   trying to set, so a retry computes a fresh `create` for exactly that value.
 * - A failed plain delete restores the row (the server still has it) at the end of its section, so
 *   the error has a row to attach to.
 * - A failed plain create needs no fixup: the row already carries the attempted value and no
 *   `permissionId`.
 *
 * The baseline advances only for rows that changed on the server, so a retry recomputes exactly
 * the outstanding work (D7).
 */
export const applyOperationResults = (
  baseline: GroupPermissionsFormValues,
  values: GroupPermissionsFormValues,
  results: OperationResult[]
): ApplyResultsOutcome => {
  let nextBaseline = baseline;
  let nextValues = values;
  const errors: RowError[] = [];
  let hasFailure = false;

  for (const { operation, outcome } of results) {
    const { sectionPath } = parseRowPath(operation.rowPath);

    if (outcome.status === "applied") {
      if (operation.kind === "create") {
        const { index } = parseRowPath(operation.rowPath);
        const rows = [...sectionOf(nextValues, sectionPath)];
        rows[index] = { ...rows[index], permissionId: outcome.permissionId };
        nextValues = withSection(nextValues, sectionPath, rows);
        nextBaseline = withSection(nextBaseline, sectionPath, [...sectionOf(nextBaseline, sectionPath), rows[index]]);
      } else if (operation.kind === "delete") {
        nextBaseline = withSection(
          nextBaseline,
          sectionPath,
          sectionOf(nextBaseline, sectionPath).filter((row) => row.permissionId !== operation.permissionId)
        );
      } else {
        const { index } = parseRowPath(operation.rowPath);
        const rows = [...sectionOf(nextValues, sectionPath)];
        rows[index] = { ...rows[index], permissionId: outcome.permissionId };
        nextValues = withSection(nextValues, sectionPath, rows);
        nextBaseline = withSection(
          nextBaseline,
          sectionPath,
          sectionOf(nextBaseline, sectionPath).map((row) =>
            row.permissionId === operation.permissionId ? rows[index] : row
          )
        );
      }
      continue;
    }

    hasFailure = true;
    errors.push({ rowPath: operation.rowPath, messageId: ROW_ERROR_MESSAGE_IDS[outcome.status] });

    if (outcome.status === "createFailed") {
      // Nothing was lost: the row stays exactly as submitted.
      continue;
    }

    if (outcome.status === "deleteFailed" && operation.kind === "delete") {
      // The row is missing from `values` (the user removed it); the server still has it. Restore
      // it at the end of its section and report the error there.
      const baselineRow = sectionOf(nextBaseline, sectionPath).find(
        (row) => row.permissionId === operation.permissionId
      );
      if (baselineRow) {
        const rows = [...sectionOf(nextValues, sectionPath), baselineRow];
        nextValues = withSection(nextValues, sectionPath, rows);
        errors[errors.length - 1] = {
          rowPath: `${sectionPath}.${rows.length - 1}`,
          messageId: ROW_ERROR_MESSAGE_IDS.deleteFailed,
        };
      }
      continue;
    }

    if (outcome.status === "deleteFailed") {
      // The delete half of a `replace` failed; the row still shows the attempted new value and a
      // retry recomputes the same replace. No fixup needed.
      continue;
    }

    // `rolledBack` and `rollbackFailed` are only ever produced for a `replace` operation
    // (`executeOperation`'s replace branch is the only place that returns them), so `permissionId`
    // is always present here.
    if (outcome.status === "rolledBack" && operation.kind === "replace") {
      const { index } = parseRowPath(operation.rowPath);
      const replacedPermissionId = operation.permissionId;
      const baselineRow = sectionOf(nextBaseline, sectionPath).find((row) => row.permissionId === replacedPermissionId);
      if (baselineRow) {
        const rows = [...sectionOf(nextValues, sectionPath)];
        rows[index] = { ...baselineRow, permissionId: outcome.permissionId };
        nextValues = withSection(nextValues, sectionPath, rows);
        nextBaseline = withSection(
          nextBaseline,
          sectionPath,
          sectionOf(nextBaseline, sectionPath).map((row) =>
            row.permissionId === replacedPermissionId ? rows[index] : row
          )
        );
      }
      continue;
    }

    if (outcome.status === "rollbackFailed" && operation.kind === "replace") {
      const { index } = parseRowPath(operation.rowPath);
      const replacedPermissionId = operation.permissionId;
      const rows = [...sectionOf(nextValues, sectionPath)];
      rows[index] = { ...rows[index], permissionId: undefined };
      nextValues = withSection(nextValues, sectionPath, rows);
      nextBaseline = withSection(
        nextBaseline,
        sectionPath,
        sectionOf(nextBaseline, sectionPath).filter((row) => row.permissionId !== replacedPermissionId)
      );
    }
  }

  return { baseline: nextBaseline, values: nextValues, errors, hasFailure };
};
