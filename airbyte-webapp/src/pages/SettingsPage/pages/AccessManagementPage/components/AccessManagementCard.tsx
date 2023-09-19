import { FormattedMessage } from "react-intl";

import { Card } from "components/ui/Card";

import { OrganizationUserRead, WorkspaceUserRead } from "core/request/AirbyteClient";

import { AccessManagementTable } from "./AccessManagementTable";
import { ResourceType, tableTitleDictionary } from "./useGetAccessManagementData";

interface AccessManagementCardProps {
  users: OrganizationUserRead[] | WorkspaceUserRead[];
  tableResourceType: ResourceType;
  pageResourceType: ResourceType;
  pageResourceName: string;
}
export const AccessManagementCard: React.FC<AccessManagementCardProps> = ({
  users,
  tableResourceType,
  pageResourceType,
  pageResourceName,
}) => {
  return (
    <Card title={<FormattedMessage id={tableTitleDictionary[tableResourceType]} />}>
      <AccessManagementTable
        users={users}
        tableResourceType={tableResourceType}
        pageResourceName={pageResourceName}
        pageResourceType={pageResourceType}
      />
    </Card>
  );
};
