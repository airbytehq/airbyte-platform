import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { RbacRole } from "core/utils/rbac/rbacPermissionsQuery";

interface ExistingUserIndicatorProps {
  highestPermissionType: RbacRole;
}

export const ExistingUserIndicator: React.FC<ExistingUserIndicatorProps> = ({ highestPermissionType }) => {
  const roleId =
    highestPermissionType === "ADMIN"
      ? "role.admin"
      : highestPermissionType === "EDITOR"
      ? "role.editor"
      : highestPermissionType === "READER"
      ? "role.reader"
      : "role.member";

  return (
    <Tooltip
      control={
        <FlexContainer alignItems="center" gap="xs">
          <Icon type="check" color="disabled" />
          <Text>
            <FormattedMessage id={roleId} />
          </Text>
        </FlexContainer>
      }
      placement="top-start"
    >
      <FormattedMessage id="userInvitations.create.modal.existingUserTooltip" />
    </Tooltip>
  );
};
