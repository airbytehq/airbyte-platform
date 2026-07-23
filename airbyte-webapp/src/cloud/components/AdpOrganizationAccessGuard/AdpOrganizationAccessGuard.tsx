import React from "react";

import { useCurrentOrganizationId, useIsAdpOrganization, useIsInstanceAdmin } from "area/organization/utils";
import { ForbiddenErrorBoundaryView } from "core/errors/components/ForbiddenErrorBoundary";
import { useExperiment, useExperimentContext } from "core/services/Experiment";

export const AdpOrganizationAccessGuard: React.FC<React.PropsWithChildren> = ({ children }) => {
  const organizationId = useCurrentOrganizationId();
  const isAdpOrganization = useIsAdpOrganization();
  const isInstanceAdmin = useIsInstanceAdmin();
  const allowAdpDataReplicationAccess = useExperiment("allowAgentsDataReplicationAccess");

  // Register here because blocked users never mount the child routes that previously owned this context.
  // A single owner also prevents child route cleanup from removing it during navigation.
  useExperimentContext("organization", organizationId);

  if (isAdpOrganization && !isInstanceAdmin && !allowAdpDataReplicationAccess) {
    return <ForbiddenErrorBoundaryView />;
  }

  return <>{children}</>;
};
