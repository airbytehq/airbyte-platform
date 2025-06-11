import { useMutation } from "@tanstack/react-query";
import { Suspense, useEffect, useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { useSearchParams } from "react-router-dom";

import { LoadingPage } from "components";
import { HeadTitle } from "components/HeadTitle";
import AirbyteLogo from "components/illustrations/airbyte-logo.svg?react";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { StepStatus, StepsIndicators } from "components/ui/StepsIndicators/StepsIndicators";

import { useCreateApplication, useNonblockingListApplications } from "core/api";
import { DefaultErrorBoundary } from "core/errors";
import { useAuthService } from "core/services/auth";

import { EmbedCodeStep } from "./components/EmbedCodeStep";
import { EmbeddedSetupFinish } from "./components/EmbeddedSetupFinish";
import { SelectEmbeddedDestination } from "./components/SelectEmbeddedDestination";
import { SetupEmbeddedDestination } from "./components/SetupEmbeddedDestination";
import styles from "./EmbeddedOnboardingPageLayout.module.scss";

export enum EmbeddedOnboardingStep {
  SelectDestination = "selectDestination",
  SetupDestination = "setupDestination",
  EmbedCode = "embedCode",
  Finish = "finish",
}

export const EMBEDDED_ONBOARDING_STEP_PARAM = "step";

export const EmbeddedOnboardingPageLayout: React.FC = () => {
  const { logout } = useAuthService();
  const { isLoading: isLogoutLoading, mutateAsync: handleLogout } = useMutation(() => logout?.() ?? Promise.resolve());
  const [searchParams, setSearchParams] = useSearchParams();
  const stepSearchParam = searchParams.get(EMBEDDED_ONBOARDING_STEP_PARAM);
  const applicationsData = useNonblockingListApplications();
  const { mutate: createApplication } = useCreateApplication();

  useEffect(() => {
    if (
      applicationsData.data !== undefined &&
      !applicationsData.isLoading &&
      Array.isArray(applicationsData.data.applications) &&
      applicationsData.data.applications.length === 0
    ) {
      // Only create application if not already present
      createApplication({ name: "Embedded Application" });
    }
  }, [applicationsData.data, applicationsData.isLoading, createApplication]);

  type StepKey = EmbeddedOnboardingStep;
  const stepOrder = useMemo<StepKey[]>(
    () => [
      EmbeddedOnboardingStep.SelectDestination,
      EmbeddedOnboardingStep.SetupDestination,
      EmbeddedOnboardingStep.EmbedCode,
      EmbeddedOnboardingStep.Finish,
    ],
    []
  );

  // Memoize currentStep and onboardingSteps for efficiency
  const currentStep = useMemo(
    () =>
      stepOrder.includes(stepSearchParam as StepKey)
        ? (stepSearchParam as StepKey)
        : EmbeddedOnboardingStep.SelectDestination,
    [stepSearchParam, stepOrder]
  );

  // If no step is specified, redirect to the first step
  useEffect(() => {
    if (!stepSearchParam) {
      // todo: infer step based on whether folks have a connection_template or not once we have all the endpoints we need https://github.com/airbytehq/airbyte-internal-issues/issues/13177
      setSearchParams({ [EMBEDDED_ONBOARDING_STEP_PARAM]: EmbeddedOnboardingStep.SelectDestination });
    }
  }, [stepSearchParam, setSearchParams]);

  const onboardingSteps = useMemo(() => {
    const steps: Record<StepKey, StepStatus> = {} as Record<StepKey, StepStatus>;
    let foundCurrent = false;
    for (const step of stepOrder) {
      if (step === currentStep) {
        steps[step] = StepStatus.ACTIVE;
        foundCurrent = true;
      } else if (!foundCurrent) {
        steps[step] = StepStatus.COMPLETE;
      } else {
        steps[step] = StepStatus.INCOMPLETE;
      }
    }
    return steps;
  }, [currentStep, stepOrder]);

  const onboardingStepsLabels = useMemo<Record<StepKey, React.ReactElement>>(
    () => ({
      [EmbeddedOnboardingStep.SelectDestination]: <FormattedMessage id="embedded.onboarding.selectDestination" />,
      [EmbeddedOnboardingStep.SetupDestination]: <FormattedMessage id="embedded.onboarding.setupDestination" />,
      [EmbeddedOnboardingStep.EmbedCode]: <FormattedMessage id="embedded.onboarding.embedCode" />,
      [EmbeddedOnboardingStep.Finish]: <FormattedMessage id="embedded.onboarding.finish" />,
    }),
    []
  );

  const steps = useMemo(
    () =>
      stepOrder.map((step) => ({
        state: onboardingSteps[step],
        label: onboardingStepsLabels[step],
      })),
    [stepOrder, onboardingSteps, onboardingStepsLabels]
  );

  let StepComponent: React.ReactElement | null = null;
  switch (currentStep) {
    case EmbeddedOnboardingStep.SelectDestination:
      StepComponent = <SelectEmbeddedDestination />;
      break;
    case EmbeddedOnboardingStep.SetupDestination:
      StepComponent = <SetupEmbeddedDestination />;
      break;
    case EmbeddedOnboardingStep.EmbedCode:
      StepComponent = <EmbedCodeStep />;
      break;
    case EmbeddedOnboardingStep.Finish:
      StepComponent = <EmbeddedSetupFinish />;
      break;
    default:
      StepComponent = null;
  }

  return (
    <>
      <HeadTitle titles={[{ id: "settings.embedded" }]} />
      <FlexContainer alignItems="center" justifyContent="space-between" className={styles.header}>
        <AirbyteLogo className={styles.logo} />
        <div className={styles.steps}>
          <StepsIndicators steps={steps} />
        </div>
        {logout && (
          <Button variant="secondary" onClick={() => handleLogout()} isLoading={isLogoutLoading}>
            <FormattedMessage id="settings.accountSettings.logoutText" />
          </Button>
        )}
      </FlexContainer>
      <DefaultErrorBoundary>
        <Suspense fallback={<LoadingPage />}>{StepComponent}</Suspense>
      </DefaultErrorBoundary>
    </>
  );
};
