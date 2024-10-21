import { FormattedMessage } from "react-intl";

import { Text } from "components/ui/Text";

import { RbacRole } from "core/utils/rbac/rbacPermissionsQuery";

export const UserRoleText: React.FC<{ highestPermissionType?: RbacRole }> = ({ highestPermissionType }) => {
  if (!highestPermissionType) {
    return null;
  }

  const roleId =
    highestPermissionType === "ADMIN"
      ? "role.admin"
      : highestPermissionType === "EDITOR"
      ? "role.editor"
      : highestPermissionType === "RUNNER"
      ? "role.runner"
      : highestPermissionType === "READER"
      ? "role.reader"
      : "role.member";

  return (
    <Text color="grey" align="center" size="sm" as="span">
      <FormattedMessage id={roleId} />
    </Text>
  );
};
