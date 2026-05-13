import React from "react";

import { useIsAdpOrganization, useIsInstanceAdmin } from "area/organization/utils";
import { ForbiddenErrorBoundaryView } from "core/errors/components/ForbiddenErrorBoundary";

export const AdpOrganizationAccessGuard: React.FC<React.PropsWithChildren> = ({ children }) => {
  const isAdpOrganization = useIsAdpOrganization();
  const isInstanceAdmin = useIsInstanceAdmin();

  if (isAdpOrganization && !isInstanceAdmin) {
    return <ForbiddenErrorBoundaryView />;
  }

  return <>{children}</>;
};
