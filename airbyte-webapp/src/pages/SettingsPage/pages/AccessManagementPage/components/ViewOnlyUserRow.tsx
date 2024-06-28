import { FormattedMessage } from "react-intl";

import { Badge } from "components/ui/Badge";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { ExistingUserIndicator } from "./ExistingUserIndicator";
import styles from "./ViewOnlyUserRow.module.scss";

interface ViewOnlyUserRowProps {
  name?: string;
  email: string;
  isCurrentUser: boolean;
  isOrgAdmin: boolean;
  highestPermissionType?: "ADMIN" | "EDITOR" | "READER" | "MEMBER";
}
export const ViewOnlyUserRow: React.FC<ViewOnlyUserRowProps> = ({
  name,
  email,
  isCurrentUser,
  isOrgAdmin,
  highestPermissionType,
}) => {
  return (
    <Box py="md" className={styles.existingUserRow}>
      <FlexContainer justifyContent="space-between" alignItems="center" className={styles.existingUserRow__content}>
        <FlexContainer direction="column" gap="none" justifyContent="center">
          <Text>
            {name}
            {isCurrentUser && (
              <Box as="span" px="sm">
                <Badge variant="grey">
                  <FormattedMessage id="settings.accessManagement.youHint" />
                </Badge>
              </Box>
            )}
          </Text>
          <Text color="grey400" italicized>
            {email}
          </Text>
        </FlexContainer>
        {isOrgAdmin && (
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
        {!isOrgAdmin && !!highestPermissionType && (
          <ExistingUserIndicator highestPermissionType={highestPermissionType} />
        )}
      </FlexContainer>
    </Box>
  );
};
