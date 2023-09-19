import { UseMutateAsyncFunction } from "@tanstack/react-query";
import React from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { CloudWorkspaceRead } from "core/api/types/CloudApi";
import { WorkspaceRead } from "core/request/AirbyteClient";

import { WorkspaceItem } from "./WorkspaceItem";
import { WorkspacesCreateControl } from "./WorkspacesCreateControl";

interface WorkspacesListProps {
  workspaces: WorkspaceRead[] | CloudWorkspaceRead[];
  createWorkspace: UseMutateAsyncFunction<WorkspaceRead | CloudWorkspaceRead, unknown, string, unknown>;
}
export const WorkspacesList: React.FC<WorkspacesListProps> = ({ workspaces, createWorkspace }) => {
  return (
    <FlexContainer direction="column">
      <WorkspacesCreateControl createWorkspace={createWorkspace} />
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
