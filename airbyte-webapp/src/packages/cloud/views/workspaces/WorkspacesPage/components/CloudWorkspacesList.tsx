import React from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { useListCloudWorkspaces } from "core/api/cloud";
import { WorkspaceItem } from "pages/workspaces/components/WorkspaceItem";

import { CloudWorkspacesCreateControl } from "./CloudWorkspacesCreateControl";

export const CloudWorkspacesList: React.FC = () => {
  const { workspaces } = useListCloudWorkspaces();

  return (
    <FlexContainer direction="column">
      <CloudWorkspacesCreateControl />
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
