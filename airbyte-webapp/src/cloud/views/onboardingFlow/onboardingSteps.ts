import { StepStatus } from "components/ui/StepsIndicators/StepsIndicators";

export const ONBOARDING_STEPS = [
  { id: "goal", labelId: "onboardingFlow.progress.goal" },
  { id: "data", labelId: "onboardingFlow.progress.data" },
  { id: "source", labelId: "onboardingFlow.progress.source" },
  { id: "destination", labelId: "onboardingFlow.progress.destination" },
  { id: "connection", labelId: "onboardingFlow.progress.connection" },
  { id: "firstSync", labelId: "onboardingFlow.progress.firstSync" },
] as const;

export type OnboardingStepId = (typeof ONBOARDING_STEPS)[number]["id"];

export const getOnboardingStepState = (stepId: OnboardingStepId, currentStepId: OnboardingStepId): StepStatus => {
  const stepIndex = ONBOARDING_STEPS.findIndex(({ id }) => id === stepId);
  const currentStepIndex = ONBOARDING_STEPS.findIndex(({ id }) => id === currentStepId);

  if (stepIndex === currentStepIndex) {
    return StepStatus.ACTIVE;
  }
  return stepIndex < currentStepIndex ? StepStatus.COMPLETE : StepStatus.INCOMPLETE;
};
