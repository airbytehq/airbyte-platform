import React from "react";

import { useOrganization } from "core/api";

interface OrganizationNameProps {
  organizationId: string;
}

export const OrganizationName: React.FC<OrganizationNameProps> = ({ organizationId }) => {
  const organization = useOrganization(organizationId);

  return <>{organization.organizationName}</>;
};
