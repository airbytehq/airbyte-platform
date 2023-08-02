import React from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { useListWorkspaces } from "core/api";

import { WorkspaceItem } from "./WorkspaceItem";
import { WorkspacesCreateControl } from "./WorkspacesCreateControl";

export const WorkspacesList: React.FC = () => {
  const { workspaces } = useListWorkspaces();
  return (
    <FlexContainer direction="column">
      <WorkspacesCreateControl />
      {workspaces.length ? (
        workspaces.map((workspace, index) => (
          <WorkspaceItem
            key={workspace.workspaceId}
            workspaceId={workspace.workspaceId}
            workspaceName={workspace.name ?? ""}
            testId={`select-workspace-${index}`}
          />
        ))
      ) : (
        <Box pt="xl">
          <Heading as="h4">
            <FormattedMessage id="workspaces.noWorkspaces" />
          </Heading>
        </Box>
      )}
    </FlexContainer>
  );
};

export default WorkspacesList;
