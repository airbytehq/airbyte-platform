import { PublicPermissionType } from "core/api/types/AirbyteClient";

import {
  applyOperationResults,
  computeOperations,
  executeOperations,
  GroupPermissionMutations,
  Operation,
  OperationContext,
  OperationResult,
} from "./groupPermissionsOperations";
import { GroupPermissionsFormValues, PermissionRowValue } from "./groupPermissionsSchema";

const context: OperationContext = { groupId: "group-1", organizationId: "org-1" };

const emptyValues = (): GroupPermissionsFormValues => ({ organizationPermission: [], workspacePermissions: [] });

const orgRow = (overrides: Partial<PermissionRowValue> = {}): PermissionRowValue => ({
  permissionId: "perm-org-1",
  permissionType: PublicPermissionType.organization_admin,
  ...overrides,
});

const workspaceRow = (overrides: Partial<PermissionRowValue> = {}): PermissionRowValue => ({
  permissionId: "perm-ws-1",
  permissionType: PublicPermissionType.workspace_admin,
  workspaceId: "workspace-1",
  ...overrides,
});

describe("computeOperations", () => {
  it("produces no operations for an empty diff", () => {
    const baseline = emptyValues();
    const values = emptyValues();

    expect(computeOperations(context, baseline, values)).toEqual([]);
  });

  it("produces no operation for an unchanged prefilled row", () => {
    const baseline: GroupPermissionsFormValues = { organizationPermission: [orgRow()], workspacePermissions: [] };
    const values: GroupPermissionsFormValues = { organizationPermission: [orgRow()], workspacePermissions: [] };

    expect(computeOperations(context, baseline, values)).toEqual([]);
  });

  it("emits a create for a row the user added, with no permissionId", () => {
    const baseline = emptyValues();
    const values: GroupPermissionsFormValues = {
      organizationPermission: [],
      workspacePermissions: [workspaceRow({ permissionId: undefined })],
    };

    const operations = computeOperations(context, baseline, values);

    expect(operations).toEqual([
      {
        kind: "create",
        rowPath: "workspacePermissions.0",
        body: {
          groupId: "group-1",
          permissionType: PublicPermissionType.workspace_admin,
          workspaceId: "workspace-1",
        },
      },
    ]);
  });

  it("emits a replace for a prefilled row whose role changed", () => {
    const baseline: GroupPermissionsFormValues = { organizationPermission: [orgRow()], workspacePermissions: [] };
    const values: GroupPermissionsFormValues = {
      organizationPermission: [orgRow({ permissionType: PublicPermissionType.organization_reader })],
      workspacePermissions: [],
    };

    const operations = computeOperations(context, baseline, values);

    expect(operations).toEqual([
      {
        kind: "replace",
        rowPath: "organizationPermission.0",
        permissionId: "perm-org-1",
        body: { groupId: "group-1", permissionType: PublicPermissionType.organization_reader, organizationId: "org-1" },
        restore: {
          groupId: "group-1",
          permissionType: PublicPermissionType.organization_admin,
          organizationId: "org-1",
        },
      },
    ]);
  });

  it("emits a delete for a baseline row removed from the current values", () => {
    const baseline: GroupPermissionsFormValues = { organizationPermission: [], workspacePermissions: [workspaceRow()] };
    const values = emptyValues();

    const operations = computeOperations(context, baseline, values);

    expect(operations).toEqual([{ kind: "delete", rowPath: "workspacePermissions.0", permissionId: "perm-ws-1" }]);
  });

  it("does not delete or rewrite a prefilled row whose permissionType is outside PublicPermissionType", () => {
    const edgeCaseRow: PermissionRowValue = {
      permissionId: "perm-edge-1",
      // `instance_admin` is a valid `PermissionType` a group permission could carry, but it is
      // outside the 11-value `PublicPermissionType` this modal can write through
      // `createGroupPermission`. Cast mirrors how a real prefill maps a wider server enum onto the
      // narrower form type.
      permissionType: "instance_admin" as PublicPermissionType,
    };
    const baseline: GroupPermissionsFormValues = { organizationPermission: [edgeCaseRow], workspacePermissions: [] };

    // Simulate an attempted removal (baseline row absent from the submitted values) and an
    // attempted rewrite (baseline row present but with a different role) — neither should ever
    // reach the API.
    const removedValues: GroupPermissionsFormValues = { organizationPermission: [], workspacePermissions: [] };
    const rewrittenValues: GroupPermissionsFormValues = {
      organizationPermission: [{ ...edgeCaseRow, permissionType: PublicPermissionType.organization_admin }],
      workspacePermissions: [],
    };

    expect(computeOperations(context, baseline, removedValues)).toEqual([]);
    expect(computeOperations(context, baseline, rewrittenValues)).toEqual([]);
  });

  it("coalesces a removed-then-re-added organization row with a different role into a single replace", () => {
    // `remove(index)` followed by `append` (the UI's only way to change the organization row) empties
    // the field array and pushes a new row with no `permissionId`. The baseline row must still be
    // classified as a `delete`+`create` pair, and that pair must coalesce into one `replace`.
    const baseline: GroupPermissionsFormValues = { organizationPermission: [orgRow()], workspacePermissions: [] };
    const values: GroupPermissionsFormValues = {
      organizationPermission: [{ permissionId: undefined, permissionType: PublicPermissionType.organization_reader }],
      workspacePermissions: [],
    };

    const operations = computeOperations(context, baseline, values);

    expect(operations).toEqual([
      {
        kind: "replace",
        rowPath: "organizationPermission.0",
        permissionId: "perm-org-1",
        body: { groupId: "group-1", permissionType: PublicPermissionType.organization_reader, organizationId: "org-1" },
        restore: {
          groupId: "group-1",
          permissionType: PublicPermissionType.organization_admin,
          organizationId: "org-1",
        },
      },
    ]);
  });

  it("emits no operation for a removed-then-re-added organization row with the same role", () => {
    const baseline: GroupPermissionsFormValues = { organizationPermission: [orgRow()], workspacePermissions: [] };
    const values: GroupPermissionsFormValues = {
      organizationPermission: [{ permissionId: undefined, permissionType: PublicPermissionType.organization_admin }],
      workspacePermissions: [],
    };

    expect(computeOperations(context, baseline, values)).toEqual([]);
  });

  it("coalesces a removed-then-re-added workspace row for the same workspace into a single replace", () => {
    const baseline: GroupPermissionsFormValues = {
      organizationPermission: [],
      workspacePermissions: [workspaceRow({ workspaceId: "workspace-1" })],
    };
    const values: GroupPermissionsFormValues = {
      organizationPermission: [],
      workspacePermissions: [
        { permissionId: undefined, permissionType: PublicPermissionType.workspace_reader, workspaceId: "workspace-1" },
      ],
    };

    const operations = computeOperations(context, baseline, values);

    expect(operations).toEqual([
      {
        kind: "replace",
        rowPath: "workspacePermissions.0",
        permissionId: "perm-ws-1",
        body: { groupId: "group-1", permissionType: PublicPermissionType.workspace_reader, workspaceId: "workspace-1" },
        restore: {
          groupId: "group-1",
          permissionType: PublicPermissionType.workspace_admin,
          workspaceId: "workspace-1",
        },
      },
    ]);
  });

  it("keeps a removed workspace W and an added, different workspace X as two independent operations", () => {
    const baseline: GroupPermissionsFormValues = {
      organizationPermission: [],
      workspacePermissions: [workspaceRow({ workspaceId: "workspace-1" })],
    };
    const values: GroupPermissionsFormValues = {
      organizationPermission: [],
      workspacePermissions: [
        { permissionId: undefined, permissionType: PublicPermissionType.workspace_admin, workspaceId: "workspace-2" },
      ],
    };

    const operations = computeOperations(context, baseline, values);

    expect(operations).toEqual([
      {
        kind: "create",
        rowPath: "workspacePermissions.0",
        body: { groupId: "group-1", permissionType: PublicPermissionType.workspace_admin, workspaceId: "workspace-2" },
      },
      { kind: "delete", rowPath: "workspacePermissions.0", permissionId: "perm-ws-1" },
    ]);
  });

  it("self-heals: a baseline row plus a permissionId-less row with a different role still produces a replace", () => {
    // Covers the case described in the plan: an identical-role removed-then-re-added row is left
    // with no `permissionId` on purpose (no operation is emitted for it). If the user edits that
    // row again, the row still has no `permissionId` and the baseline row is still absent from
    // `values` — the same shape this test constructs directly — and the coalescing rule above must
    // still turn that pair into a correct `replace`, not a bare create-plus-delete.
    const baseline: GroupPermissionsFormValues = { organizationPermission: [orgRow()], workspacePermissions: [] };
    const values: GroupPermissionsFormValues = {
      organizationPermission: [{ permissionId: undefined, permissionType: PublicPermissionType.organization_editor }],
      workspacePermissions: [],
    };

    const operations = computeOperations(context, baseline, values);

    expect(operations).toEqual([
      {
        kind: "replace",
        rowPath: "organizationPermission.0",
        permissionId: "perm-org-1",
        body: { groupId: "group-1", permissionType: PublicPermissionType.organization_editor, organizationId: "org-1" },
        restore: {
          groupId: "group-1",
          permissionType: PublicPermissionType.organization_admin,
          organizationId: "org-1",
        },
      },
    ]);
  });
});

describe("executeOperations", () => {
  const buildMutations = (overrides: Partial<GroupPermissionMutations> = {}): GroupPermissionMutations => ({
    createGroupPermission: jest.fn().mockResolvedValue({ permissionId: "new-id" }),
    deleteGroupPermission: jest.fn().mockResolvedValue(undefined),
    ...overrides,
  });

  it("issues the delete before the create for a replace operation", async () => {
    const callOrder: string[] = [];
    const mutations = buildMutations({
      deleteGroupPermission: jest.fn().mockImplementation(async () => {
        callOrder.push("delete");
      }),
      createGroupPermission: jest.fn().mockImplementation(async () => {
        callOrder.push("create");
        return { permissionId: "new-id" };
      }),
    });
    const operation: Operation = {
      kind: "replace",
      rowPath: "organizationPermission.0",
      permissionId: "perm-org-1",
      body: { groupId: "group-1", permissionType: PublicPermissionType.organization_reader, organizationId: "org-1" },
      restore: { groupId: "group-1", permissionType: PublicPermissionType.organization_admin, organizationId: "org-1" },
    };

    const results = await executeOperations([operation], mutations);

    expect(callOrder).toEqual(["delete", "create"]);
    expect(results).toEqual([{ operation, outcome: { status: "applied", permissionId: "new-id" } }]);
  });

  it("does not issue the create when the replace's delete fails", async () => {
    const createGroupPermission = jest.fn().mockResolvedValue({ permissionId: "new-id" });
    const mutations = buildMutations({
      deleteGroupPermission: jest.fn().mockRejectedValue(new Error("delete failed")),
      createGroupPermission,
    });
    const operation: Operation = {
      kind: "replace",
      rowPath: "organizationPermission.0",
      permissionId: "perm-org-1",
      body: { groupId: "group-1", permissionType: PublicPermissionType.organization_reader, organizationId: "org-1" },
      restore: { groupId: "group-1", permissionType: PublicPermissionType.organization_admin, organizationId: "org-1" },
    };

    const results = await executeOperations([operation], mutations);

    expect(createGroupPermission).not.toHaveBeenCalled();
    expect(results).toEqual([{ operation, outcome: { status: "deleteFailed" } }]);
  });

  it("issues the restore and reports rolledBack when the replace's create fails", async () => {
    const mutations = buildMutations({
      deleteGroupPermission: jest.fn().mockResolvedValue(undefined),
      createGroupPermission: jest
        .fn()
        .mockRejectedValueOnce(new Error("create failed"))
        .mockResolvedValueOnce({ permissionId: "restored-id" }),
    });
    const operation: Operation = {
      kind: "replace",
      rowPath: "organizationPermission.0",
      permissionId: "perm-org-1",
      body: { groupId: "group-1", permissionType: PublicPermissionType.organization_reader, organizationId: "org-1" },
      restore: { groupId: "group-1", permissionType: PublicPermissionType.organization_admin, organizationId: "org-1" },
    };

    const results = await executeOperations([operation], mutations);

    expect(mutations.createGroupPermission).toHaveBeenCalledTimes(2);
    expect(mutations.createGroupPermission).toHaveBeenNthCalledWith(2, operation.restore);
    expect(results).toEqual([{ operation, outcome: { status: "rolledBack", permissionId: "restored-id" } }]);
  });

  it("reports rollbackFailed when the replace's create and restore both fail", async () => {
    const mutations = buildMutations({
      deleteGroupPermission: jest.fn().mockResolvedValue(undefined),
      createGroupPermission: jest.fn().mockRejectedValue(new Error("still failing")),
    });
    const operation: Operation = {
      kind: "replace",
      rowPath: "organizationPermission.0",
      permissionId: "perm-org-1",
      body: { groupId: "group-1", permissionType: PublicPermissionType.organization_reader, organizationId: "org-1" },
      restore: { groupId: "group-1", permissionType: PublicPermissionType.organization_admin, organizationId: "org-1" },
    };

    const results = await executeOperations([operation], mutations);

    expect(results).toEqual([{ operation, outcome: { status: "rollbackFailed" } }]);
  });

  it("does not let one row's failure prevent the others from applying", async () => {
    const mutations = buildMutations({
      createGroupPermission: jest
        .fn()
        .mockRejectedValueOnce(new Error("row 1 fails"))
        .mockResolvedValueOnce({ permissionId: "row-2-id" }),
    });
    const failingCreate: Operation = {
      kind: "create",
      rowPath: "workspacePermissions.0",
      body: { groupId: "group-1", permissionType: PublicPermissionType.workspace_admin, workspaceId: "workspace-1" },
    };
    const succeedingCreate: Operation = {
      kind: "create",
      rowPath: "workspacePermissions.1",
      body: { groupId: "group-1", permissionType: PublicPermissionType.workspace_reader, workspaceId: "workspace-2" },
    };

    const results = await executeOperations([failingCreate, succeedingCreate], mutations);

    expect(results).toEqual([
      { operation: failingCreate, outcome: { status: "createFailed" } },
      { operation: succeedingCreate, outcome: { status: "applied", permissionId: "row-2-id" } },
    ]);
  });
});

describe("D7: recomputing after a partial failure", () => {
  it("yields exactly the failed work and nothing else", async () => {
    const baseline: GroupPermissionsFormValues = {
      organizationPermission: [orgRow()],
      workspacePermissions: [workspaceRow({ permissionId: "perm-ws-1", workspaceId: "workspace-1" })],
    };
    // The user changes the organization role (perm-org-1 -> reader) and adds a new workspace row.
    const submittedValues: GroupPermissionsFormValues = {
      organizationPermission: [orgRow({ permissionType: PublicPermissionType.organization_reader })],
      workspacePermissions: [
        workspaceRow({ permissionId: "perm-ws-1", workspaceId: "workspace-1" }),
        workspaceRow({ permissionId: undefined, workspaceId: "workspace-2" }),
      ],
    };

    const firstOperations = computeOperations(context, baseline, submittedValues);
    expect(firstOperations).toHaveLength(2);

    // The organization replace fails outright (delete fails); the new workspace create succeeds.
    const mutations: GroupPermissionMutations = {
      deleteGroupPermission: jest.fn().mockRejectedValue(new Error("delete failed")),
      createGroupPermission: jest.fn().mockResolvedValue({ permissionId: "new-ws-id" }),
    };
    const firstResults = await executeOperations(firstOperations, mutations);

    const outcome = applyOperationResults(baseline, submittedValues, firstResults);
    expect(outcome.hasFailure).toBe(true);

    // Recomputing against the advanced baseline and fixed-up values should reissue only the
    // organization replace — the workspace create already applied and must not repeat.
    const secondOperations = computeOperations(context, outcome.baseline, outcome.values);

    expect(secondOperations).toEqual([
      {
        kind: "replace",
        rowPath: "organizationPermission.0",
        permissionId: "perm-org-1",
        body: {
          groupId: "group-1",
          permissionType: PublicPermissionType.organization_reader,
          organizationId: "org-1",
        },
        restore: {
          groupId: "group-1",
          permissionType: PublicPermissionType.organization_admin,
          organizationId: "org-1",
        },
      },
    ]);
  });
});

describe("applyOperationResults", () => {
  it("sets the new permissionId on a row created successfully", () => {
    const baseline = emptyValues();
    const values: GroupPermissionsFormValues = {
      organizationPermission: [],
      workspacePermissions: [workspaceRow({ permissionId: undefined })],
    };
    const results: OperationResult[] = [
      {
        operation: {
          kind: "create",
          rowPath: "workspacePermissions.0",
          body: {
            groupId: "group-1",
            permissionType: PublicPermissionType.workspace_admin,
            workspaceId: "workspace-1",
          },
        },
        outcome: { status: "applied", permissionId: "new-id" },
      },
    ];

    const outcome = applyOperationResults(baseline, values, results);

    expect(outcome.hasFailure).toBe(false);
    expect(outcome.values.workspacePermissions[0].permissionId).toBe("new-id");
    expect(outcome.baseline.workspacePermissions).toEqual([workspaceRow({ permissionId: "new-id" })]);
  });

  it("restores a row whose plain delete failed, at the end of its section, with an error attached", () => {
    const baseline: GroupPermissionsFormValues = { organizationPermission: [], workspacePermissions: [workspaceRow()] };
    const values = emptyValues();
    const results: OperationResult[] = [
      {
        operation: { kind: "delete", rowPath: "workspacePermissions.0", permissionId: "perm-ws-1" },
        outcome: { status: "deleteFailed" },
      },
    ];

    const outcome = applyOperationResults(baseline, values, results);

    expect(outcome.hasFailure).toBe(true);
    expect(outcome.values.workspacePermissions).toEqual([workspaceRow()]);
    expect(outcome.errors).toEqual([
      { rowPath: "workspacePermissions.0", messageId: "settings.organization.groups.editPermissions.row.deleteFailed" },
    ]);
    // The delete never happened, so the baseline is untouched.
    expect(outcome.baseline).toEqual(baseline);
  });

  it("resets a rolledBack row to its previous role and updates its permissionId", () => {
    const baseline: GroupPermissionsFormValues = { organizationPermission: [orgRow()], workspacePermissions: [] };
    const values: GroupPermissionsFormValues = {
      organizationPermission: [orgRow({ permissionType: PublicPermissionType.organization_reader })],
      workspacePermissions: [],
    };
    const results: OperationResult[] = [
      {
        operation: {
          kind: "replace",
          rowPath: "organizationPermission.0",
          permissionId: "perm-org-1",
          body: {
            groupId: "group-1",
            permissionType: PublicPermissionType.organization_reader,
            organizationId: "org-1",
          },
          restore: {
            groupId: "group-1",
            permissionType: PublicPermissionType.organization_admin,
            organizationId: "org-1",
          },
        },
        outcome: { status: "rolledBack", permissionId: "restored-id" },
      },
    ];

    const outcome = applyOperationResults(baseline, values, results);

    expect(outcome.hasFailure).toBe(true);
    expect(outcome.values.organizationPermission[0]).toEqual(orgRow({ permissionId: "restored-id" }));
    expect(outcome.baseline.organizationPermission[0]).toEqual(orgRow({ permissionId: "restored-id" }));
  });

  it("clears the permissionId on a rollbackFailed row and keeps the attempted value", () => {
    const baseline: GroupPermissionsFormValues = { organizationPermission: [orgRow()], workspacePermissions: [] };
    const values: GroupPermissionsFormValues = {
      organizationPermission: [orgRow({ permissionType: PublicPermissionType.organization_reader })],
      workspacePermissions: [],
    };
    const results: OperationResult[] = [
      {
        operation: {
          kind: "replace",
          rowPath: "organizationPermission.0",
          permissionId: "perm-org-1",
          body: {
            groupId: "group-1",
            permissionType: PublicPermissionType.organization_reader,
            organizationId: "org-1",
          },
          restore: {
            groupId: "group-1",
            permissionType: PublicPermissionType.organization_admin,
            organizationId: "org-1",
          },
        },
        outcome: { status: "rollbackFailed" },
      },
    ];

    const outcome = applyOperationResults(baseline, values, results);

    expect(outcome.hasFailure).toBe(true);
    expect(outcome.values.organizationPermission[0]).toEqual({
      permissionId: undefined,
      permissionType: PublicPermissionType.organization_reader,
    });
    expect(outcome.baseline.organizationPermission).toEqual([]);
  });
});
