import React from "react";

import { useCurrentWorkspace } from "core/api";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";

import { AccessManagementPageContent } from "./components/AccessManagementPageContent";
import { useGetWorkspaceAccessUsers } from "./components/useGetAccessManagementData";

export const WorkspaceAccessManagementPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.SETTINGS_WORKSPACE_ACCESS_MANAGEMENT);
  const { name } = useCurrentWorkspace();
  const workspaceAccessUsers = useGetWorkspaceAccessUsers();

  return (
    <AccessManagementPageContent resourceName={name} accessUsers={workspaceAccessUsers} pageResourceType="workspace" />
  );
};
