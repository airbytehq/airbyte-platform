import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { ExternalLink } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { OrganizationRead } from "core/api/types/AirbyteClient";

import styles from "./NoWorkspacesPermissionWarning.module.scss";

export const NoWorkspacePermissionsContent: React.FC<{ organizations: OrganizationRead[] }> = ({ organizations }) => {
  return (
    <Box my="2xl" data-testid="noWorkspacePermissionsBanner">
      <FlexContainer alignItems="center" direction="column" gap="2xl">
        <FlexContainer alignItems="center" justifyContent="center" className={styles.circle}>
          <Icon type="folderOpen" className={styles.icon} />
        </FlexContainer>
        <FlexContainer alignItems="center" direction="column" gap="sm">
          <Text size="lg" bold>
            <FormattedMessage id="workspaces.noPermissions" />
          </Text>
          <Text color="grey400">
            <FormattedMessage
              id="workspaces.noPermissions.moreInformation"
              values={{
                adminEmail: organizations[0].email,
                lnk: (...lnk: React.ReactNode[]) => (
                  <ExternalLink href={`mailto:${organizations[0].email}`}>{lnk}</ExternalLink>
                ),
              }}
            />
          </Text>
        </FlexContainer>
      </FlexContainer>
    </Box>
  );
};
