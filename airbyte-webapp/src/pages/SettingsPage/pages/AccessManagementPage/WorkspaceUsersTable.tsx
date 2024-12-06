import { createColumnHelper } from "@tanstack/react-table";
import { useMemo } from "react";
import { FormattedMessage } from "react-intl";

import { Table } from "components/ui/Table";

import { useCurrentUser } from "core/services/auth";
import { FeatureItem, useFeature } from "core/services/features";
import { RbacRoleHierarchy } from "core/utils/rbac/rbacPermissionsQuery";

import { RoleManagementCell } from "./components/RoleManagementCell";
import { UserCell } from "./components/UserCell";
import { getWorkspaceAccessLevel, UnifiedUserModel } from "./components/util";

export const WorkspaceUsersTable: React.FC<{
  users: UnifiedUserModel[];
}> = ({ users }) => {
  const { userId: currentUserId } = useCurrentUser();
  const areAllRbacRolesEnabled = useFeature(FeatureItem.AllowAllRBACRoles);

  const columnHelper = createColumnHelper<UnifiedUserModel>();

  const columns = useMemo(
    () => [
      columnHelper.accessor("userName", {
        header: () => <FormattedMessage id="settings.accessManagement.table.column.member" />,
        cell: (props) => {
          return (
            <UserCell
              name={props.row.original.userName}
              email={props.row.original.userEmail}
              isCurrentUser={props.row.original.id === currentUserId}
              uniqueId={props.row.original.id}
            />
          );
        },
        sortingFn: "alphanumeric",
      }),
      columnHelper.accessor(
        (row) => {
          return getWorkspaceAccessLevel(row);
        },
        {
          id: "permissionType",
          header: () => (
            <>
              <FormattedMessage id="resource.workspace" />{" "}
              <FormattedMessage id="settings.accessManagement.table.column.role" />
            </>
          ),
          cell: (props) => {
            return <RoleManagementCell user={props.row.original} resourceType="workspace" />;
          },
          enableSorting: !!areAllRbacRolesEnabled,
          sortingFn: (a, b, order) => {
            const aHighestRole = getWorkspaceAccessLevel(a.original);
            const bHighestRole = getWorkspaceAccessLevel(b.original);

            const aValue = RbacRoleHierarchy.indexOf(aHighestRole);
            const bValue = RbacRoleHierarchy.indexOf(bHighestRole);

            if (order === "asc") {
              return aValue - bValue;
            }
            return bValue - aValue;
          },
        }
      ),
    ],
    [areAllRbacRolesEnabled, columnHelper, currentUserId]
  );

  return (
    <Table data={users} columns={columns} initialSortBy={[{ id: "userName", desc: false }]} stickyHeaders={false} />
  );
};
