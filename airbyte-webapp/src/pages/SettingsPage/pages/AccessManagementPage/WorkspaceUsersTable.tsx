import { createColumnHelper } from "@tanstack/react-table";
import { useMemo } from "react";
import { FormattedMessage } from "react-intl";

import { Table } from "components/ui/Table";

import { useCurrentUser } from "core/services/auth";
import { RbacRoleHierarchy } from "core/utils/rbac/rbacPermissionsQuery";

import { NextAccessUserRead, getWorkspaceAccessLevel } from "./components/useGetAccessManagementData";
import { UserCell } from "./components/UserCell";
import { RoleManagementMenu } from "./next/RoleManagementMenu";

export const WorkspaceUsersTable: React.FC<{
  users: NextAccessUserRead[];
}> = ({ users }) => {
  const { userId: currentUserId } = useCurrentUser();
  const columnHelper = createColumnHelper<NextAccessUserRead>();

  const columns = useMemo(
    () => [
      columnHelper.accessor("name", {
        header: () => <FormattedMessage id="settings.accessManagement.table.column.member" />,
        cell: (props) => {
          return (
            <UserCell
              name={props.row.original.name}
              email={props.row.original.email}
              isCurrentUser={props.row.original.userId === currentUserId}
              userId={props.row.original.userId}
            />
          );
        },
        sortingFn: "alphanumeric",
        meta: { responsive: true },
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
          meta: { responsive: true },
          cell: (props) => {
            return <RoleManagementMenu user={props.row.original} resourceType="workspace" />;
          },
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
    [columnHelper, currentUserId]
  );

  return <Table data={users} columns={columns} initialSortBy={[{ id: "name", desc: false }]} />;
};
