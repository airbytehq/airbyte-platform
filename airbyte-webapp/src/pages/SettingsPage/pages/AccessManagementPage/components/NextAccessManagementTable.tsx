import { createColumnHelper } from "@tanstack/react-table";
import { useMemo } from "react";
import { FormattedMessage } from "react-intl";

import { Badge } from "components/ui/Badge";
import { FlexContainer } from "components/ui/Flex";
import { Table } from "components/ui/Table";

import { useCurrentUser } from "core/services/auth";

import { NextAccessUserRead, ResourceType, getHighestPermissionType } from "./useGetAccessManagementData";
import { RoleManagementMenu } from "../next/RoleManagementMenu";

export const NextAccessManagementTable: React.FC<{
  users: NextAccessUserRead[];
  tableResourceType: ResourceType;
  pageResourceType: ResourceType;
  pageResourceName: string;
}> = ({ users, tableResourceType }) => {
  const { userId: currentUserId } = useCurrentUser();
  const columnHelper = createColumnHelper<NextAccessUserRead>();

  const columns = useMemo(
    () => [
      columnHelper.accessor("name", {
        header: () => <FormattedMessage id="settings.accessManagement.table.column.fullname" />,
        cell: (props) => {
          return (
            <FlexContainer direction="row" alignItems="baseline">
              {props.cell.getValue()}
              {props.row.original.userId === currentUserId && (
                <Badge variant="grey">
                  <FormattedMessage id="settings.accessManagement.youHint" />
                </Badge>
              )}
            </FlexContainer>
          );
        },
        sortingFn: "alphanumeric",
        meta: { responsive: true },
      }),
      columnHelper.accessor("email", {
        header: () => <FormattedMessage id="settings.accessManagement.table.column.email" />,
        cell: (props) => props.cell.getValue(),
        sortingFn: "alphanumeric",
        meta: { responsive: true },
      }),
      columnHelper.accessor(
        (row) => {
          return getHighestPermissionType(row, tableResourceType);
        },
        {
          id: "permissionType",
          header: () => (
            <>
              <FormattedMessage
                id={tableResourceType === "workspace" ? "resource.workspace" : "resource.organization"}
              />{" "}
              <FormattedMessage id="settings.accessManagement.table.column.role" />
            </>
          ),
          meta: { responsive: true },
          cell: (props) => {
            return <RoleManagementMenu user={props.row.original} resourceType={tableResourceType} />;
          },
          sortingFn: (a, b, order) => {
            const roleOrder = ["admin", "editor", "reader", "member", undefined];
            const aHighestRole = getHighestPermissionType(a.original, tableResourceType);
            const bHighestRole = getHighestPermissionType(b.original, tableResourceType);

            const aValue = aHighestRole === undefined ? -1 : roleOrder.indexOf(aHighestRole);
            const bValue = bHighestRole === undefined ? -1 : roleOrder.indexOf(bHighestRole);

            if (order === "asc") {
              return aValue - bValue;
            }
            return bValue - aValue;
          },
        }
      ),
    ],
    [columnHelper, tableResourceType, currentUserId]
  );

  return <Table data={users} columns={columns} initialSortBy={[{ id: "name", desc: false }]} />;
};
