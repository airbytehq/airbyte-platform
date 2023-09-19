import React from "react";

import { useCurrentWorkspace, useOrganization } from "core/api";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";

import { AccessManagementPageContent } from "./components/AccessManagementPageContent";
import { useGetOrganizationAccessUsers } from "./components/useGetAccessManagementData";

export const OrganizationAccessManagementPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.SETTINGS_ORGANIZATION_ACCESS_MANAGEMENT);
  const workspace = useCurrentWorkspace();
  const organizationId = workspace.organizationId ?? "";
  const { organizationName } = useOrganization(organizationId);
  const organizationAccessUsers = useGetOrganizationAccessUsers();

  return (
    <AccessManagementPageContent
      resourceName={organizationName}
      accessUsers={organizationAccessUsers}
      pageResourceType="organization"
    />
  );
};
