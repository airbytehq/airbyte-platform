import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { ExternalLink } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { OrganizationRead } from "core/api/types/AirbyteClient";

import styles from "./NoWorkspacesPermissionWarning.module.scss";
import OctaviaThinking from "./octavia-thinking-no-gears.svg?react";

export const NoWorkspacePermissionsContent: React.FC<{ organizations: OrganizationRead[] }> = ({ organizations }) => {
  return (
    <Box m="2xl" p="2xl" data-testid="noWorkspacePermissionsBanner">
      <FlexContainer direction="column" gap="2xl">
        <OctaviaThinking className={styles.cloudWorkspacesPage__illustration} />
        <div>
          <Box pb="md">
            <Text size="md" align="center" bold>
              <FormattedMessage id="workspaces.noPermissions" />
            </Text>
          </Box>
          <Text size="md" align="center" color="grey">
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
        </div>
      </FlexContainer>
    </Box>
  );
};
