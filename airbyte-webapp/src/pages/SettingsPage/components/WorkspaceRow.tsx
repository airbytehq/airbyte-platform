import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import styles from "./WorkspaceRow.module.scss";

interface WorkspaceRowProps {
  workspaceName: string;
}

export const WorkspaceRow: React.FC<WorkspaceRowProps> = ({ workspaceName }) => {
  return (
    <FlexContainer className={styles.workspaceRow} alignItems="center" gap="md">
      <Text size="sm">{workspaceName}</Text>
    </FlexContainer>
  );
};
