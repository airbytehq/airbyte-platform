import { Navigate } from "react-router-dom";

import { useListEmbeddedOrganizations } from "core/api";
import { OnboardingStatusEnum } from "core/api/types/SonarClient";
import { DefaultErrorBoundary } from "core/errors";
import { RoutePaths } from "pages/routePaths";

import {
  EmbeddedOnboardingPageLayout,
  EMBEDDED_ONBOARDING_STEP_PARAM,
  EmbeddedOnboardingStep,
} from "./EmbeddedOnboardingPageLayout";
import { EmbeddedUpsell } from "./EmbeddedUpsell";
import { WORKSPACE_ID_PARAM } from "../EmbeddedSourceCreatePage/hooks/useEmbeddedSourceParams";

export const EmbeddedOnboardingPage: React.FC = () => {
  return (
    <DefaultErrorBoundary>
      <EmbeddedOnboardingPageLayout />
    </DefaultErrorBoundary>
  );
};

export const EmbeddedOnboardingRedirect: React.FC = () => {
  const { organizations } = useListEmbeddedOrganizations();

  const organizationsWithOnboardingRemaining = organizations.filter(
    (org) => org.onboarding_status !== OnboardingStatusEnum.COMPLETED
  );

  if (!organizations) {
    return (
      <DefaultErrorBoundary>
        <EmbeddedUpsell />
      </DefaultErrorBoundary>
    );
  }

  if (organizationsWithOnboardingRemaining.length === 0) {
    // If no organizations are found, redirect to main view
    return <Navigate to="/" replace />;
  }

  const organizationToUse = organizationsWithOnboardingRemaining[0];
  const searchParams = new URLSearchParams();

  if (organizationToUse.onboarding_status === OnboardingStatusEnum.NOT_STARTED) {
    searchParams.set(EMBEDDED_ONBOARDING_STEP_PARAM, EmbeddedOnboardingStep.SelectDestination);
    searchParams.set(WORKSPACE_ID_PARAM, organizationToUse.first_workspace_id);
  } else if (organizationToUse.onboarding_status === OnboardingStatusEnum.DESTINATION_SETUP_COMPLETE) {
    searchParams.set(EMBEDDED_ONBOARDING_STEP_PARAM, EmbeddedOnboardingStep.EmbedCode);
    searchParams.set(WORKSPACE_ID_PARAM, organizationToUse.first_workspace_id);
  } else if (organizationToUse.onboarding_status === OnboardingStatusEnum.EMBED_CODE_COPIED) {
    searchParams.set(EMBEDDED_ONBOARDING_STEP_PARAM, EmbeddedOnboardingStep.Finish);
    searchParams.set(WORKSPACE_ID_PARAM, organizationToUse.first_workspace_id);
  }

  const targetUrl = `/${RoutePaths.Organization}/${organizationToUse.organization_id}/${
    RoutePaths.EmbeddedOnboarding
  }?${searchParams.toString()}`;

  return <Navigate to={targetUrl} replace />;
};
