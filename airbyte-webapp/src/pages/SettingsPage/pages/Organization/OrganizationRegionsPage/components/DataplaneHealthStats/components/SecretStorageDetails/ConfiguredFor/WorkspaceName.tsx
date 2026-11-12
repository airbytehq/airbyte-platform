import React from "react";

import { useGetWorkspace } from "core/api";

interface WorkspaceNameProps {
  workspaceId: string;
}

export const WorkspaceName: React.FC<WorkspaceNameProps> = ({ workspaceId }) => {
  const workspace = useGetWorkspace(workspaceId);

  return <>{workspace?.name ?? workspaceId}</>;
};
