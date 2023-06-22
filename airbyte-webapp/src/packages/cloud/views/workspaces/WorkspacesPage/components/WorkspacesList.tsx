import React from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { useListCloudWorkspaces } from "core/api/cloud";

import { WorkspaceItem } from "./WorkspaceItem";

export const WorkspacesList: React.FC = () => {
  const { workspaces } = useListCloudWorkspaces();

  if (!workspaces.length) {
    return (
      <Heading as="h4">
        <FormattedMessage id="workspaces.noWorkspaces" />
      </Heading>
    );
  }

  return (
    <FlexContainer direction="column">
      {workspaces.map((workspace, index) => (
        <WorkspaceItem
          key={workspace.workspaceId}
          workspaceId={workspace.workspaceId}
          workspaceName={workspace.name ?? ""}
          testId={`select-workspace-${index}`}
        />
      ))}
    </FlexContainer>
  );
};

export default WorkspacesList;
