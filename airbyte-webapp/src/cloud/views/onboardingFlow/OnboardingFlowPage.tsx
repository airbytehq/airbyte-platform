import React from "react";

import LoadingPage from "components/ui/LoadingPage";

import { useRegisterWorkspaceContext } from "cloud/area/workspace/utils/useRegisterWorkspaceContext";
import { AdpOrganizationAccessGuard } from "cloud/components/AdpOrganizationAccessGuard";
import { useCurrentWorkspace } from "core/api";
import { usePrefetchWorkspaceData } from "core/api/cloud";
import { DefaultErrorBoundary, ForbiddenErrorBoundary } from "core/errors";

import { OnboardingActionBar } from "./OnboardingActionBar";
import styles from "./OnboardingFlowPage.module.scss";
import { OnboardingHeader } from "./OnboardingHeader";

const OnboardingFlow: React.FC = () => {
  // This route is outside MainLayout and WorkspacesRoutes, so it sets up what the flow needs from them.
  usePrefetchWorkspaceData();
  const workspace = useCurrentWorkspace();
  useRegisterWorkspaceContext(workspace);

  return (
    <div className={styles.page}>
      <OnboardingHeader currentStep="goal" />
      <main className={styles.page__body}>
        <AdpOrganizationAccessGuard>
          <div className={styles.page__content}>
            <OnboardingActionBar />
          </div>
        </AdpOrganizationAccessGuard>
      </main>
    </div>
  );
};

const OnboardingFlowPage: React.FC = () => (
  <DefaultErrorBoundary>
    <ForbiddenErrorBoundary>
      <React.Suspense fallback={<LoadingPage />}>
        <OnboardingFlow />
      </React.Suspense>
    </ForbiddenErrorBoundary>
  </DefaultErrorBoundary>
);

export default OnboardingFlowPage;
