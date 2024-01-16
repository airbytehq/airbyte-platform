import { FormattedMessage } from "react-intl";

import { Text } from "components/ui/Text";

export const UserRoleText: React.FC<{ highestPermissionType?: "admin" | "editor" | "reader" | "member" }> = ({
  highestPermissionType,
}) => {
  if (!highestPermissionType) {
    return null;
  }

  const roleId =
    highestPermissionType === "admin"
      ? "role.admin"
      : highestPermissionType === "editor"
      ? "role.editor"
      : highestPermissionType === "reader"
      ? "role.reader"
      : "role.member";

  return (
    <Text color="grey" align="center">
      <FormattedMessage id={roleId} />
    </Text>
  );
};
