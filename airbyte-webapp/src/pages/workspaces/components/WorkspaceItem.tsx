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
  ref: React.Ref<HTMLAnchorElement>;
}

export const WorkspaceItem: React.FC<WorkspaceItemProps> = React.forwardRef(
  ({ workspaceId, workspaceName, testId }, ref: React.Ref<HTMLAnchorElement> | undefined) => {
    return (
      <Link to={`/workspaces/${workspaceId}`} data-testid={testId} variant="primary" ref={ref}>
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

          <Icon type="chevronRight" size="lg" color="action" className={styles.workspaceItem__workspaceButtonCaret} />
        </FlexContainer>
      </Link>
    );
  }
);

WorkspaceItem.displayName = "WorkspaceItem";
