import { createColumnHelper } from "@tanstack/react-table";
import { useMemo, useState } from "react";
import { FormattedMessage } from "react-intl";

import { Table } from "components/ui/Table";

import { OrganizationUserRead, WorkspaceUserRead } from "core/request/AirbyteClient";

import { RoleManagementControl } from "./RoleManagementControl";
import { RoleToolTip } from "./RoleToolTip";
import { ResourceType } from "./useGetAccessManagementData";

export const AccessManagementTable: React.FC<{
  users: WorkspaceUserRead[] | OrganizationUserRead[];
  tableResourceType: ResourceType;
  pageResourceType: ResourceType;
  pageResourceName: string;
}> = ({ users, tableResourceType, pageResourceType, pageResourceName }) => {
  const columnHelper = createColumnHelper<WorkspaceUserRead | OrganizationUserRead>();
  const [activeEditRow, setActiveEditRow] = useState<string | undefined>(undefined);

  const columns = useMemo(
    () => [
      columnHelper.accessor("name", {
        header: () => <FormattedMessage id="settings.accessManagement.table.column.fullname" />,
        cell: (props) => props.cell.getValue(),
        sortingFn: "alphanumeric",
        meta: { responsive: true },
      }),
      columnHelper.accessor("email", {
        header: () => <FormattedMessage id="settings.accessManagement.table.column.email" />,
        cell: (props) => props.cell.getValue(),
        sortingFn: "alphanumeric",
        meta: { responsive: true },
      }),
      columnHelper.accessor("permissionType", {
        header: () => (
          <>
            <FormattedMessage id="settings.accessManagement.table.column.role" />
            <RoleToolTip resourceType={tableResourceType} />
          </>
        ),
        meta: { responsive: true },
        cell: (props) => {
          let workspaceId;
          let organizationId;

          const { userId, permissionType, permissionId, name } = props.row.original;
          if ("organizationId" in props.row.original) {
            organizationId = props.row.original.organizationId;
          }
          if ("workspaceId" in props.row.original) {
            workspaceId = props.row.original.workspaceId;
          }
          return (
            <RoleManagementControl
              userId={userId}
              userName={name}
              resourceName={pageResourceName}
              permissionType={permissionType}
              permissionId={permissionId}
              workspaceId={workspaceId}
              organizationId={organizationId}
              tableResourceType={tableResourceType}
              pageResourceType={pageResourceType}
              activeEditRow={activeEditRow}
              setActiveEditRow={setActiveEditRow}
            />
          );
        },
        sortingFn: "alphanumeric",
      }),
    ],
    [activeEditRow, columnHelper, pageResourceName, pageResourceType, tableResourceType]
  );

  return <Table data={users} columns={columns} variant="white" />;
};
