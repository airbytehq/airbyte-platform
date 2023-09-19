import React from "react";

import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { Link } from "components/ui/Link";

import styles from "./WorkspaceItem.module.scss";

interface WorkspaceItemProps {
  workspaceId: string;
  workspaceName: string;
  testId: string;
}

export const WorkspaceItem: React.FC<WorkspaceItemProps> = ({ workspaceId, workspaceName, testId }) => {
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
        </FlexContainer>

        <Icon type="chevronRight" size="lg" color="affordance" className={styles.workspaceItem__workspaceButtonCaret} />
      </FlexContainer>
    </Link>
  );
};
