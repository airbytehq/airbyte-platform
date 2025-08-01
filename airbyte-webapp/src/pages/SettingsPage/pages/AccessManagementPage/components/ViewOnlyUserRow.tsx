import { FormattedMessage } from "react-intl";

import { Badge } from "components/ui/Badge";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { ScopeType } from "core/api/types/AirbyteClient";
import { RbacRole } from "core/utils/rbac/rbacPermissionsQuery";

import { ExistingUserIndicator } from "./ExistingUserIndicator";
import { PendingInvitationBadge } from "./RoleManagementCell";
import styles from "./ViewOnlyUserRow.module.scss";

interface ViewOnlyUserRowProps {
  name?: string;
  email: string;
  isCurrentUser: boolean;
  isOrgAdmin: boolean;
  highestPermissionType?: RbacRole;
  scope: ScopeType;
  isPending: boolean;
}
export const ViewOnlyUserRow: React.FC<ViewOnlyUserRowProps> = ({
  name,
  email,
  isCurrentUser,
  isOrgAdmin,
  highestPermissionType,
  scope,
  isPending,
}) => {
  return (
    <Box py="md" className={styles.existingUserRow}>
      <FlexContainer justifyContent="space-between" alignItems="center" className={styles.existingUserRow__content}>
        <FlexContainer direction="column" gap="none" justifyContent="center">
          {name && (
            <FlexContainer gap="sm" alignItems="center">
              <Text>{name}</Text>
              {isCurrentUser && (
                <Badge variant="grey">
                  <FormattedMessage id="settings.accessManagement.youHint" />
                </Badge>
              )}
            </FlexContainer>
          )}
          <FlexContainer gap="sm" alignItems="center">
            <Text color="grey400" italicized>
              {email}
            </Text>
            {isCurrentUser && !name && (
              <Badge variant="grey">
                <FormattedMessage id="settings.accessManagement.youHint" />
              </Badge>
            )}
          </FlexContainer>
        </FlexContainer>
        <FlexContainer alignItems="center">
          {isOrgAdmin && scope !== ScopeType.organization && (
            <Tooltip
              control={
                <Badge variant="grey">
                  <FormattedMessage id="role.organizationAdmin" />
                </Badge>
              }
              placement="top-start"
            >
              <FormattedMessage id="userInvitations.create.modal.organizationAdminTooltip" />
            </Tooltip>
          )}
          {(!isOrgAdmin || scope !== ScopeType.workspace) && !!highestPermissionType && (
            <ExistingUserIndicator highestPermissionType={highestPermissionType} scope={scope} />
          )}
          {isPending && <PendingInvitationBadge scope={scope} />}
        </FlexContainer>
      </FlexContainer>
    </Box>
  );
};
