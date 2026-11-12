import React from "react";
import { useIntl } from "react-intl";
import { Navigate } from "react-router-dom";

import { AdminWorkspaceWarning } from "components/ui/AdminWorkspaceWarning";
import LoadingPage from "components/ui/LoadingPage";

import { useCurrentOrganizationId } from "area/organization/utils";
import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useRegisterWorkspaceContext } from "cloud/area/workspace/utils/useRegisterWorkspaceContext";
import { AdpOrganizationAccessGuard } from "cloud/components/AdpOrganizationAccessGuard";
import { useCurrentWorkspace, useUpdateOrganizationOnboarding } from "core/api";
import { usePrefetchWorkspaceData } from "core/api/cloud";
import { DefaultErrorBoundary, ForbiddenErrorBoundary } from "core/errors";
import { FeatureItem, IfFeatureEnabled } from "core/services/features";
import { useNotificationService } from "core/services/Notification";
import { RoutePaths } from "pages/routePaths";

import styles from "./OnboardingFlowPage.module.scss";
import { OnboardingHeader } from "./OnboardingHeader";
import { OnboardingScreens, useOnboardingScreenState } from "./OnboardingScreens";
import { PROGRESS_STEP_BY_SCREEN } from "./onboardingScreenSteps";
import { useCanOpenOnboardingFlow } from "./useShouldEnterOnboardingFlow";

const useSkipOnboarding = () => {
  const organizationId = useCurrentOrganizationId();
  const { mutate, isLoading } = useUpdateOrganizationOnboarding();
  const { registerNotification } = useNotificationService();
  const { formatMessage } = useIntl();

  // On success the cached status becomes "skipped", and OnboardingFlowGate opens the normal app.
  const skip = () =>
    mutate(
      { organizationId, status: "skipped" },
      {
        onError: () =>
          registerNotification({
            id: "onboardingFlow/skip-failure",
            text: formatMessage({ id: "onboardingFlow.actions.skipError" }),
            type: "error",
          }),
      }
    );

  return { skip, isSkipping: isLoading };
};

const OnboardingFlow: React.FC = () => {
  // This route is outside MainLayout and WorkspacesRoutes, so it sets up what the flow needs from them.
  usePrefetchWorkspaceData();
  const workspace = useCurrentWorkspace();
  useRegisterWorkspaceContext(workspace);
  const { skip, isSkipping } = useSkipOnboarding();
  const screenState = useOnboardingScreenState();

  return (
    <div className={styles.page}>
      <IfFeatureEnabled feature={FeatureItem.ShowAdminWarningInWorkspace}>
        <AdminWorkspaceWarning />
      </IfFeatureEnabled>
      <OnboardingHeader currentStep={PROGRESS_STEP_BY_SCREEN[screenState.screen]} />
      <main className={styles.page__body}>
        <AdpOrganizationAccessGuard>
          <div className={styles.page__content}>
            <fieldset disabled={isSkipping} className={styles.page__screens}>
              <OnboardingScreens state={screenState} onSkip={skip} />
            </fieldset>
          </div>
        </AdpOrganizationAccessGuard>
      </main>
    </div>
  );
};

/** Skip lands here too: the update changes the cached status, and this sends the user to the normal app. */
const OnboardingFlowGate: React.FC = () => {
  const canOpenFlow = useCanOpenOnboardingFlow();
  const workspaceId = useCurrentWorkspaceId();

  if (!canOpenFlow) {
    return <Navigate to={`/${RoutePaths.Workspaces}/${workspaceId}`} replace />;
  }
  return <OnboardingFlow />;
};

const OnboardingFlowPage: React.FC = () => (
  <DefaultErrorBoundary>
    <ForbiddenErrorBoundary>
      <React.Suspense fallback={<LoadingPage />}>
        <OnboardingFlowGate />
      </React.Suspense>
    </ForbiddenErrorBoundary>
  </DefaultErrorBoundary>
);

export default OnboardingFlowPage;
