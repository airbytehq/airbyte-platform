import React from "react";

import { useCreateCloudWorkspace } from "core/api/cloud";
import { WorkspacesPageContent } from "pages/workspaces/WorkspacesPage";

export const CloudWorkspacesPage: React.FC = () => {
  const { mutateAsync: createWorkspace } = useCreateCloudWorkspace();

  return <WorkspacesPageContent createWorkspace={createWorkspace} />;
};
