import { FormattedMessage } from "react-intl";

import { Card } from "components/ui/Card";

import { OrganizationUserRead, WorkspaceUserRead } from "core/request/AirbyteClient";

import { AccessManagementTable } from "./AccessManagementTable";
import { ResourceType, tableTitleDictionary } from "./useGetAccessManagementData";

interface AccessManagementCardProps {
  users: OrganizationUserRead[] | WorkspaceUserRead[];
  resourceType: ResourceType;
}
export const AccessManagementCard: React.FC<AccessManagementCardProps> = ({ users, resourceType }) => {
  return (
    <Card title={<FormattedMessage id={tableTitleDictionary[resourceType]} />}>
      <AccessManagementTable users={users} tableResourceType={resourceType} />
    </Card>
  );
};
