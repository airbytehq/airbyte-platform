import React from "react";

import { useListCloudWorkspaces } from "packages/cloud/services/workspaces/CloudWorkspacesService";

import { WorkspaceItem } from "./WorkspaceItem";
import styles from "./WorkspacesList.module.scss";

const WorkspacesList: React.FC = () => {
  const workspaces = useListCloudWorkspaces();

  return (
    <>
      {workspaces.length && (
        <ul className={styles.container}>
          {workspaces.map((workspace, index) => (
            <li>
              <WorkspaceItem
                key={workspace.workspaceId}
                workspaceId={workspace.workspaceId}
                workspaceName={workspace.name}
                testId={`select-workspace-${index}`}
              />
            </li>
          ))}
        </ul>
      )}
    </>
  );
};

export default WorkspacesList;
