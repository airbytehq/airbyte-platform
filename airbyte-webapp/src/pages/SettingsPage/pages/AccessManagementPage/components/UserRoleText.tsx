import { FormattedMessage } from "react-intl";

import { Text } from "components/ui/Text";

import { RbacRole } from "core/utils/rbac/rbacPermissionsQuery";

/**
 * Keyed on RbacRole so a newly added role is a compile error here rather than silently falling
 * through to the "Member" label.
 */
const roleMessageIds: Record<RbacRole, string> = {
  ADMIN: "role.admin",
  EDITOR: "role.editor",
  SOURCE_EDITOR: "role.sourceEditor",
  DESTINATION_EDITOR: "role.destinationEditor",
  RUNNER: "role.runner",
  READER: "role.reader",
  MEMBER: "role.member",
};

export const UserRoleText: React.FC<{ highestPermissionType?: RbacRole }> = ({ highestPermissionType }) => {
  if (!highestPermissionType) {
    return null;
  }

  const roleId = roleMessageIds[highestPermissionType];

  return (
    <Text color="grey" align="center" size="sm" as="span">
      <FormattedMessage id={roleId} />
    </Text>
  );
};
