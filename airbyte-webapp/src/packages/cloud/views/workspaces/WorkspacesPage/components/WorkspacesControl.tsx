import React from "react";
import { FormattedMessage } from "react-intl";
import { useToggle } from "react-use";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";

import CreateWorkspaceForm from "./CreateWorkspaceForm";
import styles from "./WorkspaceControl.module.scss";

export const WorkspacesControl: React.FC<{
  onSubmit: (name: string) => Promise<unknown>;
}> = (props) => {
  const [isEditMode, toggleMode] = useToggle(false);

  const onSubmit = async (values: { name: string }) => {
    await props.onSubmit(values.name);
    toggleMode();
  };

  return (
    <Box pb="2xl">
      {isEditMode ? (
        <Card withPadding>
          <CreateWorkspaceForm onSubmit={onSubmit} />
        </Card>
      ) : (
        <Button className={styles.createButton} onClick={toggleMode} data-testid="workspaces.createNew">
          <FormattedMessage id="workspaces.createNew" />
        </Button>
      )}
    </Box>
  );
};
