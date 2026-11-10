import React from "react";
import { Navigate, Outlet } from "react-router-dom";

import { useCurrentOrganizationId } from "area/organization/utils";
import { useCurrentWorkspaceId } from "area/workspace/utils";
import { CloudRoutes } from "cloud/cloudRoutePaths";
import { useDefaultWorkspaceInOrganization } from "core/api";
import { RoutePaths } from "pages/routePaths";

import { useShouldEnterOnboardingFlow } from "./useShouldEnterOnboardingFlow";

/**
 * Sends org editors and admins of an organization in onboarding to the flow until they skip or complete. Organization
 * pages have no workspace ID, so the redirect uses the organization's first workspace.
 */
const OnboardingFlowRedirect: React.FC = () => {
  const currentWorkspaceId = useCurrentWorkspaceId();
  const organizationId = useCurrentOrganizationId();
  const isOrganizationPage = !currentWorkspaceId;
  const defaultWorkspace = useDefaultWorkspaceInOrganization(organizationId, isOrganizationPage);
  const workspaceId = currentWorkspaceId || defaultWorkspace?.workspaceId;

  if (!workspaceId) {
    return <Outlet />;
  }
  return <Navigate to={`/${RoutePaths.Workspaces}/${workspaceId}/${CloudRoutes.OnboardingFlow}`} replace />;
};

export const OnboardingGuard: React.FC = () => {
  const shouldEnterFlow = useShouldEnterOnboardingFlow();

  return shouldEnterFlow ? <OnboardingFlowRedirect /> : <Outlet />;
};
