import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, getByRole, getByTestId, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { render } from "test-utils";

import {
  useCreateGroupPermission,
  useDeleteGroupPermission,
  useListGroupPermissions,
  useListWorkspacesInOrganization,
} from "core/api";
import { GroupPermissionReadList, GroupRead, PermissionType, PublicPermissionType } from "core/api/types/AirbyteClient";

import { EditGroupPermissionsModal } from "./EditGroupPermissionsModal";

// Minimal mock, not `{ ...jest.requireActual("core/api"), ... }`: the real barrel pulls in
// `useCurrentOrganizationId` and the auth/config chain behind it, which is not set up in this
// test. `groupKeys.permissionList` only needs to be a function the component can call; its exact
// output is not asserted here.
jest.mock("core/api", () => ({
  groupKeys: { permissionList: (groupId: string) => ["scope:organization", "groups", "permissions", groupId] },
  useListGroupPermissions: jest.fn(),
  useCreateGroupPermission: jest.fn(),
  useDeleteGroupPermission: jest.fn(),
  useListWorkspacesInOrganization: jest.fn(),
}));

const mockUseListGroupPermissions = useListGroupPermissions as jest.Mock;
const mockUseCreateGroupPermission = useCreateGroupPermission as jest.Mock;
const mockUseDeleteGroupPermission = useDeleteGroupPermission as jest.Mock;
const mockUseListWorkspacesInOrganization = useListWorkspacesInOrganization as jest.Mock;

const group: GroupRead = {
  groupId: "group-1",
  name: "Data team",
  description: null,
  organizationId: "org-1",
  memberCount: 2,
};

const workspace1 = { workspaceId: "workspace-1", name: "Workspace One" };
const workspace2 = { workspaceId: "workspace-2", name: "Workspace Two" };
const workspace3 = { workspaceId: "workspace-3", name: "Workspace Three" };

const setPermissionsQuery = (state: {
  data?: GroupPermissionReadList;
  isInitialLoading?: boolean;
  isError?: boolean;
}) =>
  mockUseListGroupPermissions.mockReturnValue({
    data: undefined,
    isInitialLoading: false,
    isError: false,
    ...state,
  });

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const setWorkspacesQuery = (workspaces: any[]) =>
  mockUseListWorkspacesInOrganization.mockReturnValue({ data: { pages: [{ workspaces }] } });

let createGroupPermissionMock: jest.Mock;
let deleteGroupPermissionMock: jest.Mock;

const selectDropdownOption = (container: HTMLElement, testId: string, optionName: string) => {
  fireEvent.click(getByTestId(container, `${testId}-listbox-button`));
  const optionsMenu = screen.getByTestId(`${testId}-listbox-options`);
  fireEvent.click(getByRole(optionsMenu, "option", { name: optionName }));
};

beforeEach(() => {
  jest.clearAllMocks();
  createGroupPermissionMock = jest.fn().mockResolvedValue({ permissionId: "new-id" });
  deleteGroupPermissionMock = jest.fn().mockResolvedValue(undefined);
  mockUseCreateGroupPermission.mockReturnValue({ mutateAsync: createGroupPermissionMock });
  mockUseDeleteGroupPermission.mockReturnValue({ mutateAsync: deleteGroupPermissionMock });
  setWorkspacesQuery([workspace1, workspace2, workspace3]);
  setPermissionsQuery({ data: { permissions: [] } });
});

const renderModal = async (onComplete = jest.fn(), onCancel = jest.fn(), queryClient = new QueryClient()) => {
  const result = await render(
    <QueryClientProvider client={queryClient}>
      <EditGroupPermissionsModal group={group} organizationId="org-1" onCancel={onCancel} onComplete={onComplete} />
    </QueryClientProvider>
  );
  return { ...result, queryClient, onComplete, onCancel };
};

const saveButton = () => screen.getByRole("button", { name: "Save" });

describe(`${EditGroupPermissionsModal.name}`, () => {
  it("renders an inline error, without throwing, when the read fails (403)", async () => {
    setPermissionsQuery({ isError: true });

    await renderModal();

    expect(screen.getByText("Something went wrong loading permissions. Please try again.")).toBeInTheDocument();
  });

  it("prefills both sections from listGroupPermissions", async () => {
    setPermissionsQuery({
      data: {
        permissions: [
          {
            permissionId: "perm-org-1",
            groupId: group.groupId,
            permissionType: PermissionType.organization_admin,
            organizationId: "org-1",
          },
          {
            permissionId: "perm-ws-1",
            groupId: group.groupId,
            permissionType: PermissionType.workspace_admin,
            workspaceId: "workspace-1",
          },
        ],
      },
    });

    const { container } = await renderModal();

    expect(getByTestId(container, "organizationPermission.0.permissionType-listbox-button")).toHaveTextContent("Admin");
    expect(getByTestId(container, "workspacePermissions.0.workspaceId-listbox-button")).toHaveTextContent(
      "Workspace One"
    );
    expect(getByTestId(container, "workspacePermissions.0.permissionType-listbox-button")).toHaveTextContent("Admin");
  });

  it("disables Save on open, and re-disables it once a change is reverted", async () => {
    setPermissionsQuery({
      data: {
        permissions: [
          {
            permissionId: "perm-org-1",
            groupId: group.groupId,
            permissionType: PermissionType.organization_admin,
            organizationId: "org-1",
          },
        ],
      },
    });

    const { container } = await renderModal();

    expect(saveButton()).toBeDisabled();

    selectDropdownOption(container, "organizationPermission.0.permissionType", "Reader");
    await waitFor(() => expect(saveButton()).toBeEnabled());

    selectDropdownOption(container, "organizationPermission.0.permissionType", "Admin");
    await waitFor(() => expect(saveButton()).toBeDisabled());
  });

  it("disables, but still renders, the organization add button once a row exists", async () => {
    setPermissionsQuery({
      data: {
        permissions: [
          {
            permissionId: "perm-org-1",
            groupId: group.groupId,
            permissionType: PermissionType.organization_admin,
            organizationId: "org-1",
          },
        ],
      },
    });

    await renderModal();

    // Both sections' add buttons share an accessible name; the organization section renders
    // first, and the workspace section (empty here) is never disabled.
    const [organizationAddButton] = screen.getAllByRole("button", { name: "Add permission" });
    expect(organizationAddButton).toBeInTheDocument();
    expect(organizationAddButton).toBeDisabled();
  });

  it("omits the column headers when a section is empty", async () => {
    await renderModal();

    expect(screen.queryByText("Organization Role")).not.toBeInTheDocument();
    expect(screen.queryByText("Workspace Role")).not.toBeInTheDocument();
  });

  it("excludes a workspace already chosen in another row from this row's options (D5)", async () => {
    setPermissionsQuery({
      data: {
        permissions: [
          {
            permissionId: "perm-ws-1",
            groupId: group.groupId,
            permissionType: PermissionType.workspace_admin,
            workspaceId: "workspace-1",
          },
          {
            permissionId: "perm-ws-2",
            groupId: group.groupId,
            permissionType: PermissionType.workspace_reader,
            workspaceId: "workspace-2",
          },
        ],
      },
    });

    const { container } = await renderModal();

    fireEvent.click(getByTestId(container, "workspacePermissions.0.workspaceId-listbox-button"));
    const optionsMenu = screen.getByTestId("workspacePermissions.0.workspaceId-listbox-options");

    expect(getByRole(optionsMenu, "option", { name: "Workspace One" })).toBeInTheDocument();
    expect(getByRole(optionsMenu, "option", { name: "Workspace Three" })).toBeInTheDocument();
    expect(screen.queryByRole("option", { name: "Workspace Two" })).not.toBeInTheDocument();
  });

  it("applies a full success, closes the modal, and invalidates the permissions query", async () => {
    setPermissionsQuery({
      data: {
        permissions: [
          {
            permissionId: "perm-org-1",
            groupId: group.groupId,
            permissionType: PermissionType.organization_admin,
            organizationId: "org-1",
          },
        ],
      },
    });

    const queryClient = new QueryClient();
    const invalidateSpy = jest.spyOn(queryClient, "invalidateQueries");
    const onComplete = jest.fn();

    const { container } = await renderModal(onComplete, jest.fn(), queryClient);

    selectDropdownOption(container, "organizationPermission.0.permissionType", "Reader");
    await waitFor(() => expect(saveButton()).toBeEnabled());

    await userEvent.click(saveButton());

    await waitFor(() => expect(onComplete).toHaveBeenCalled());
    expect(deleteGroupPermissionMock).toHaveBeenCalledWith({ groupId: group.groupId, permissionId: "perm-org-1" });
    expect(createGroupPermissionMock).toHaveBeenCalledWith({
      groupId: group.groupId,
      permissionType: PublicPermissionType.organization_reader,
      organizationId: "org-1",
    });
    expect(invalidateSpy).toHaveBeenCalledTimes(1);
  });

  it("invalidates the permissions query once per save, not once per operation", async () => {
    setPermissionsQuery({
      data: {
        permissions: [
          {
            permissionId: "perm-org-1",
            groupId: group.groupId,
            permissionType: PermissionType.organization_admin,
            organizationId: "org-1",
          },
          {
            permissionId: "perm-ws-1",
            groupId: group.groupId,
            permissionType: PermissionType.workspace_admin,
            workspaceId: "workspace-1",
          },
          {
            permissionId: "perm-ws-2",
            groupId: group.groupId,
            permissionType: PermissionType.workspace_admin,
            workspaceId: "workspace-2",
          },
        ],
      },
    });

    const queryClient = new QueryClient();
    const invalidateSpy = jest.spyOn(queryClient, "invalidateQueries");
    const onComplete = jest.fn();

    const { container } = await renderModal(onComplete, jest.fn(), queryClient);

    selectDropdownOption(container, "organizationPermission.0.permissionType", "Reader");
    selectDropdownOption(container, "workspacePermissions.0.permissionType", "Reader");
    selectDropdownOption(container, "workspacePermissions.1.permissionType", "Reader");
    await waitFor(() => expect(saveButton()).toBeEnabled());

    await userEvent.click(saveButton());

    await waitFor(() => expect(onComplete).toHaveBeenCalled());
    // Three rows changed, so three deletes and three creates were issued...
    expect(deleteGroupPermissionMock).toHaveBeenCalledTimes(3);
    expect(createGroupPermissionMock).toHaveBeenCalledTimes(3);
    // ...and the query was invalidated once, after all six had settled.
    expect(invalidateSpy).toHaveBeenCalledTimes(1);
  });

  it("invalidates the permissions query on a partial failure too, since the server still changed", async () => {
    setPermissionsQuery({
      data: {
        permissions: [
          {
            permissionId: "perm-ws-1",
            groupId: group.groupId,
            permissionType: PermissionType.workspace_admin,
            workspaceId: "workspace-1",
          },
          {
            permissionId: "perm-ws-2",
            groupId: group.groupId,
            permissionType: PermissionType.workspace_admin,
            workspaceId: "workspace-2",
          },
        ],
      },
    });
    deleteGroupPermissionMock.mockImplementation(({ permissionId }: { permissionId: string }) =>
      permissionId === "perm-ws-1" ? Promise.reject(new Error("delete failed")) : Promise.resolve(undefined)
    );

    const queryClient = new QueryClient();
    const invalidateSpy = jest.spyOn(queryClient, "invalidateQueries");
    const onComplete = jest.fn();

    const { container } = await renderModal(onComplete, jest.fn(), queryClient);

    selectDropdownOption(container, "workspacePermissions.0.permissionType", "Reader");
    selectDropdownOption(container, "workspacePermissions.1.permissionType", "Reader");
    await waitFor(() => expect(saveButton()).toBeEnabled());

    await userEvent.click(saveButton());

    await waitFor(() => expect(screen.getByText("Could not remove this permission.")).toBeInTheDocument());
    expect(onComplete).not.toHaveBeenCalled();
    expect(invalidateSpy).toHaveBeenCalledTimes(1);
  });

  it("keeps the modal open on a partial failure, reports the error under the failing row only, and leaves Save enabled", async () => {
    setPermissionsQuery({
      data: {
        permissions: [
          {
            permissionId: "perm-ws-1",
            groupId: group.groupId,
            permissionType: PermissionType.workspace_admin,
            workspaceId: "workspace-1",
          },
          {
            permissionId: "perm-ws-2",
            groupId: group.groupId,
            permissionType: PermissionType.workspace_admin,
            workspaceId: "workspace-2",
          },
        ],
      },
    });
    // Row 0 (workspace-1) fails; row 1 (workspace-2) succeeds.
    deleteGroupPermissionMock.mockImplementation(({ permissionId }: { permissionId: string }) =>
      permissionId === "perm-ws-1" ? Promise.reject(new Error("delete failed")) : Promise.resolve(undefined)
    );

    const onComplete = jest.fn();
    const { container } = await renderModal(onComplete);

    selectDropdownOption(container, "workspacePermissions.0.permissionType", "Reader");
    selectDropdownOption(container, "workspacePermissions.1.permissionType", "Reader");
    await waitFor(() => expect(saveButton()).toBeEnabled());

    await userEvent.click(saveButton());

    await waitFor(() => expect(screen.getByText("Could not remove this permission.")).toBeInTheDocument());
    expect(onComplete).not.toHaveBeenCalled();
    expect(saveButton()).toBeEnabled();

    // The error is scoped to the failing row: the succeeded row shows no error text near it, and
    // its dropdown now reflects the applied change.
    expect(getByTestId(container, "workspacePermissions.1.permissionType-listbox-button")).toHaveTextContent("Reader");
  });

  it("coalesces a remove-then-add on the organization row into one delete-then-create, and restores on a failed create", async () => {
    setPermissionsQuery({
      data: {
        permissions: [
          {
            permissionId: "perm-org-1",
            groupId: group.groupId,
            permissionType: PermissionType.organization_admin,
            organizationId: "org-1",
          },
        ],
      },
    });
    // The replace's create fails; the restore (a second create, with the original role) succeeds.
    createGroupPermissionMock.mockRejectedValueOnce(new Error("create failed"));
    createGroupPermissionMock.mockResolvedValueOnce({ permissionId: "restored-id" });

    const { container } = await renderModal();

    // Remove the prefilled row, then add a new one — the same sequence `OrganizationPermissionSection`
    // drives through `remove(index)` followed by `append`.
    fireEvent.click(screen.getByRole("button", { name: "Remove permission" }));
    const [organizationAddButton] = screen.getAllByRole("button", { name: "Add permission" });
    fireEvent.click(organizationAddButton);
    selectDropdownOption(container, "organizationPermission.0.permissionType", "Reader");
    await waitFor(() => expect(saveButton()).toBeEnabled());

    await userEvent.click(saveButton());

    await waitFor(() =>
      expect(
        screen.getByText("Could not change this permission. The previous role is still in place.")
      ).toBeInTheDocument()
    );

    // One delete, issued before the (first) create — not two independent, concurrently fired calls.
    expect(deleteGroupPermissionMock).toHaveBeenCalledTimes(1);
    expect(deleteGroupPermissionMock).toHaveBeenCalledWith({ groupId: group.groupId, permissionId: "perm-org-1" });
    expect(createGroupPermissionMock).toHaveBeenCalledTimes(2);
    expect(createGroupPermissionMock).toHaveBeenNthCalledWith(2, {
      groupId: group.groupId,
      permissionType: PublicPermissionType.organization_admin,
      organizationId: "org-1",
    });
    const deleteCallOrder = deleteGroupPermissionMock.mock.invocationCallOrder[0];
    const firstCreateCallOrder = createGroupPermissionMock.mock.invocationCallOrder[0];
    expect(deleteCallOrder).toBeLessThan(firstCreateCallOrder);
  });

  it("reissues only the failed row on a second submit after a partial failure (D7)", async () => {
    setPermissionsQuery({
      data: {
        permissions: [
          {
            permissionId: "perm-ws-1",
            groupId: group.groupId,
            permissionType: PermissionType.workspace_admin,
            workspaceId: "workspace-1",
          },
          {
            permissionId: "perm-ws-2",
            groupId: group.groupId,
            permissionType: PermissionType.workspace_admin,
            workspaceId: "workspace-2",
          },
        ],
      },
    });
    deleteGroupPermissionMock.mockImplementation(({ permissionId }: { permissionId: string }) =>
      permissionId === "perm-ws-1" ? Promise.reject(new Error("delete failed")) : Promise.resolve(undefined)
    );

    const { container } = await renderModal();

    selectDropdownOption(container, "workspacePermissions.0.permissionType", "Reader");
    selectDropdownOption(container, "workspacePermissions.1.permissionType", "Reader");
    await waitFor(() => expect(saveButton()).toBeEnabled());

    await userEvent.click(saveButton());
    await waitFor(() => expect(screen.getByText("Could not remove this permission.")).toBeInTheDocument());

    createGroupPermissionMock.mockClear();
    deleteGroupPermissionMock.mockClear();
    deleteGroupPermissionMock.mockImplementation(({ permissionId }: { permissionId: string }) =>
      permissionId === "perm-ws-1" ? Promise.reject(new Error("delete failed again")) : Promise.resolve(undefined)
    );

    await userEvent.click(saveButton());

    await waitFor(() => expect(deleteGroupPermissionMock).toHaveBeenCalledTimes(1));
    expect(deleteGroupPermissionMock).toHaveBeenCalledWith({ groupId: group.groupId, permissionId: "perm-ws-1" });
    expect(createGroupPermissionMock).not.toHaveBeenCalled();
  });
});
