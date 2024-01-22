import { FormattedMessage } from "react-intl";

import { Badge } from "components/ui/Badge";

import { PermissionType } from "core/api/types/AirbyteClient";

import { permissionStringDictionary } from "./useGetAccessManagementData";
export const RoleIndicator: React.FC<{ permissionType?: PermissionType }> = ({ permissionType }) => (
  <Badge variant="grey">
    {permissionType ? (
      <>
        <FormattedMessage id={permissionStringDictionary[permissionType].resource} />{" "}
        <FormattedMessage id={permissionStringDictionary[permissionType].role} />
      </>
    ) : (
      <FormattedMessage id="role.guest" />
    )}
  </Badge>
);
