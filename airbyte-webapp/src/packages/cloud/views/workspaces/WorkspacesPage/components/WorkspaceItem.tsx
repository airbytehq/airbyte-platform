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
      <FlexContainer direction="row" alignItems="center" justifyContent="space-between" className={styles.button}>
        <Heading as="h2" size="sm">
          {workspaceName}
        </Heading>
        <Icon type="chevronRight" size="xl" />
      </FlexContainer>
    </Link>
  );
};
