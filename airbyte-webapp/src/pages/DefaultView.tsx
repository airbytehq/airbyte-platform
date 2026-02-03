import { Navigate } from "react-router-dom";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";
import { useListWorkspacesInfinite } from "core/api";
import { useCurrentUser } from "core/services/auth";
import { useExperiment } from "core/services/Experiment";
import { useFeature } from "core/services/features/FeatureService";
import { FeatureItem } from "core/services/features/types";
import { useLocalStorage } from "core/utils/useLocalStorage";
import { useRedirectFromChatConnectorBuilder } from "core/utils/useRedirectFromChatConnectorBuilder";
import { RoutePaths } from "pages/routePaths";

export const DefaultView: React.FC = () => {
  const { data: workspacesData } = useListWorkspacesInfinite(2, "", true);
  const workspaces = workspacesData?.pages.flatMap((page) => page.data.workspaces) ?? [];
  const organizationId = useCurrentOrganizationId();
  const isSurveyEnabled = useExperiment("onboarding.surveyEnabled");
  const showOrganizationUI = useFeature(FeatureItem.OrganizationUI);
  const user = useCurrentUser();
  const [isNewSignup] = useLocalStorage("airbyte_new-signup", false);
  const connectorNavigationUrl = useRedirectFromChatConnectorBuilder(workspaces[0]?.workspaceId);

  if (connectorNavigationUrl) {
    return <Navigate to={connectorNavigationUrl} replace />;
  }

  // Check if user should be redirected to onboarding
  // Only redirect if: survey is enabled AND user hasn't completed/skipped onboarding AND they have a workspace
  const onboardingStatus = user.metadata?.onboarding as string | undefined;
  const needsOnboarding =
    isNewSignup && (!onboardingStatus || (onboardingStatus !== "completed" && onboardingStatus !== "skipped"));
  if (isSurveyEnabled && needsOnboarding && workspaces[0]?.workspaceId) {
    return <Navigate to={`/${RoutePaths.Workspaces}/${workspaces[0].workspaceId}/onboarding`} replace />;
  }

  const getNavigationPath = () => {
    // if we have multiple workspaces and org UI is disabled, go to first workspace
    if (workspaces.length > 0 && !showOrganizationUI) {
      return `/${RoutePaths.Workspaces}/${workspaces[0].workspaceId}`;
    }

    // if one workspace, go directly to it
    if (workspaces.length === 1) {
      return `/${RoutePaths.Workspaces}/${workspaces[0].workspaceId}`;
    }

    // if multiple workspaces, navigate to org workspaces page
    return `/${RoutePaths.Organization}/${organizationId}/${RoutePaths.Workspaces}`;
  };

  return <Navigate to={getNavigationPath()} replace />;
};

export default DefaultView;
