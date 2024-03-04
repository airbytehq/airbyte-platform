import React, { useEffect } from "react";
import { useInView } from "react-intersection-observer";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { LoadingSpinner } from "components/ui/LoadingSpinner";

import { WorkspaceRead } from "core/api/types/AirbyteClient";
import { CloudWorkspaceRead } from "core/api/types/CloudApi";

import { WorkspaceItem } from "./WorkspaceItem";

interface WorkspacesListProps {
  workspaces: WorkspaceRead[] | CloudWorkspaceRead[];
  fetchNextPage: () => void;
  hasNextPage?: boolean;
  isLoading?: boolean;
}
export const WorkspacesList: React.FC<WorkspacesListProps> = ({
  workspaces,
  fetchNextPage,
  hasNextPage,
  isLoading,
}) => {
  const { ref, inView } = useInView();

  useEffect(() => {
    if (inView && hasNextPage) {
      fetchNextPage();
    }
  }, [inView, fetchNextPage, hasNextPage]);

  if (isLoading) {
    return (
      <Box py="2xl">
        <FlexContainer justifyContent="center">
          <LoadingSpinner />
        </FlexContainer>
      </Box>
    );
  }

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
