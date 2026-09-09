import React from "react";
import { matchPath, useLocation } from "react-router-dom";

import { useCurrentOrganizationId, useIsAdpOrganization, useIsInstanceAdmin } from "area/organization/utils";
import { CloudSettingsRoutePaths } from "cloud/views/settings/routePaths";
import { ForbiddenErrorBoundaryView } from "core/errors/components/ForbiddenErrorBoundary";
import { useExperiment, useExperimentContext } from "core/services/Experiment";
import { RoutePaths } from "pages/routePaths";

export const AdpOrganizationAccessGuard: React.FC<React.PropsWithChildren> = ({ children }) => {
  const organizationId = useCurrentOrganizationId();
  const isAdpOrganization = useIsAdpOrganization();
  const isInstanceAdmin = useIsInstanceAdmin();
  const allowAdpDataReplicationAccess = useExperiment("allowAgentsDataReplicationAccess");
  const { pathname } = useLocation();
  const isContextLayerRoute =
    matchPath(
      `/${RoutePaths.Organization}/:organizationId/${RoutePaths.Settings}/${CloudSettingsRoutePaths.ContextLayer}`,
      pathname
    ) !== null ||
    matchPath(
      `/${RoutePaths.Workspaces}/:workspaceId/${RoutePaths.Settings}/${CloudSettingsRoutePaths.ContextLayer}`,
      pathname
    ) !== null;

  // Register here because blocked users never mount the child routes that previously owned this context.
  // A single owner also prevents child route cleanup from removing it during navigation.
  useExperimentContext("organization", organizationId);

  if (isAdpOrganization && !isInstanceAdmin && !allowAdpDataReplicationAccess && !isContextLayerRoute) {
    return <ForbiddenErrorBoundaryView />;
  }

  return <>{children}</>;
};
