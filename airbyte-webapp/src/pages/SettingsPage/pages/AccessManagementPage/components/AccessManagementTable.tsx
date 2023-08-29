import { createColumnHelper } from "@tanstack/react-table";
import { useMemo } from "react";
import { FormattedMessage } from "react-intl";

import { Table } from "components/ui/Table";

import { OrganizationUserRead, WorkspaceUserRead } from "core/request/AirbyteClient";

import { RoleToolTip } from "./RoleToolTip";
import { ResourceType, permissionStringDictionary } from "./useGetAccessManagementData";

export const AccessManagementTable: React.FC<{
  users: WorkspaceUserRead[] | OrganizationUserRead[];
  tableResourceType: ResourceType;
}> = ({ users, tableResourceType }) => {
  const columnHelper = createColumnHelper<WorkspaceUserRead | OrganizationUserRead>();

  const columns = useMemo(
    () => [
      columnHelper.accessor("name", {
        header: () => <FormattedMessage id="settings.accessManagement.table.column.fullname" />,
        cell: (props) => props.cell.getValue(),
        sortingFn: "alphanumeric",
      }),
      columnHelper.accessor("email", {
        header: () => <FormattedMessage id="settings.accessManagement.table.column.email" />,
        cell: (props) => props.cell.getValue(),
        sortingFn: "alphanumeric",
      }),
      columnHelper.accessor("permissionType", {
        header: () => (
          <>
            <FormattedMessage id="settings.accessManagement.table.column.role" />
            <RoleToolTip resourceType={tableResourceType} />
          </>
        ),
        cell: (props) => <FormattedMessage id={`${permissionStringDictionary[props.cell.getValue()]}`} />,
        sortingFn: "alphanumeric",
      }),
    ],
    [columnHelper, tableResourceType]
  );

  return <Table data={users} columns={columns} variant="white" />;
};
