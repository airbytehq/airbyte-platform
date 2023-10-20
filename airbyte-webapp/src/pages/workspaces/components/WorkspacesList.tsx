import React, { useEffect } from "react";
import { useInView } from "react-intersection-observer";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { CloudWorkspaceRead } from "core/api/types/CloudApi";
import { WorkspaceRead } from "core/request/AirbyteClient";

import { WorkspaceItem } from "./WorkspaceItem";

interface WorkspacesListProps {
  workspaces: WorkspaceRead[] | CloudWorkspaceRead[];
  fetchNextPage: () => void;
  hasNextPage?: boolean;
}
export const WorkspacesList: React.FC<WorkspacesListProps> = ({ workspaces, fetchNextPage, hasNextPage }) => {
  const { ref, inView } = useInView();

  useEffect(() => {
    if (inView && hasNextPage) {
      fetchNextPage();
    }
  }, [inView, fetchNextPage, hasNextPage]);

  return (
    <FlexContainer direction="column">
      {workspaces.length ? (
        workspaces.map((workspace, index) => (
          <WorkspaceItem
            key={workspace.workspaceId}
            workspaceId={workspace.workspaceId}
            workspaceName={workspace.name ?? ""}
            testId={`select-workspace-${index}`}
            ref={index === workspaces.length - 5 ? ref : null}
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
