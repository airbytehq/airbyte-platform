import classNames from "classnames";
import React from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { Link } from "components/ui/Link";
import { Text } from "components/ui/Text";

import styles from "./WorkspaceItem.module.scss";

interface WorkspaceItemProps {
  workspaceId: string;
  orgName?: string;
  workspaceName: string;
  testId: string;
  permissions?: string;
}

/*
  We do not yet have APIs for fetching org name or permissions.  However, I included them here and they
  can be experimented with in the console and/or storybook.
*/

export const WorkspaceItem: React.FC<WorkspaceItemProps> = ({
  workspaceId,
  workspaceName,
  orgName,
  permissions,
  testId,
}) => {
  const roleId = permissions ? `role.${permissions.toLowerCase()}` : "";

  return (
    <Link to={`/workspaces/${workspaceId}`} data-testid={testId} variant="primary">
      <FlexContainer
        direction="row"
        alignItems="center"
        justifyContent="space-between"
        className={styles.workspaceItem__button}
      >
        <FlexContainer direction="column" gap="sm" className={styles.workspaceItem__workspaceButtonTextContent}>
          <Heading as="h2" size="md" className={styles.workspaceItem__workspaceName}>
            {workspaceName}
          </Heading>
          <FlexContainer alignItems="baseline">
            {orgName && (
              <Text size="lg" color="grey">
                {orgName}
              </Text>
            )}
            {permissions && (
              <Box
                py="xs"
                px="md"
                mb="sm"
                className={classNames({
                  [styles.workspaceItem__memberPill]: permissions === "Member",
                  [styles.workspaceItem__adminPill]: permissions === "Admin",
                })}
              >
                <Text size="sm" color={permissions === "Member" ? "green600" : "blue"}>
                  <FormattedMessage id="user.roleLabel" values={{ role: <FormattedMessage id={roleId} /> }} />
                </Text>
              </Box>
            )}
          </FlexContainer>
        </FlexContainer>

        <Icon type="chevronRight" size="lg" color="affordance" className={styles.workspaceItem__workspaceButtonCaret} />
      </FlexContainer>
    </Link>
  );
};
