import { Navigate } from "react-router-dom";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";
import { useListWorkspacesInfinite } from "core/api";
import { useCurrentUser } from "core/services/auth";
import { FeatureItem, useFeature } from "core/services/features";
import { useLocalStorage } from "core/utils/useLocalStorage";
import { useRedirectFromChatConnectorBuilder } from "core/utils/useRedirectFromChatConnectorBuilder";
import { useExperiment } from "hooks/services/Experiment";
import { RoutePaths } from "pages/routePaths";

export const DefaultView: React.FC = () => {
  const { data: workspacesData } = useListWorkspacesInfinite(2, "", true);
  const workspaces = workspacesData?.pages.flatMap((page) => page.data.workspaces) ?? [];
  const organizationId = useCurrentOrganizationId();
  const multiWorkspaceUI = useFeature(FeatureItem.MultiWorkspaceUI);
  const isOrgPickerEnabled = useExperiment("sidebar.showOrgPicker");
  const isSurveyEnabled = useExperiment("onboarding.surveyEnabled");
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

  return (
    <Navigate
      to={
        multiWorkspaceUI && workspaces.length !== 1
          ? isOrgPickerEnabled
            ? `/${RoutePaths.Organization}/${organizationId}/${RoutePaths.Workspaces}`
            : `/${RoutePaths.Workspaces}`
          : `/${RoutePaths.Workspaces}/${workspaces[0]?.workspaceId}`
      }
      replace
    />
  );
};

export default DefaultView;
