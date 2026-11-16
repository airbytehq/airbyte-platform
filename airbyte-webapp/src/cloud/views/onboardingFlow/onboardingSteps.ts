import { StepStatus } from "components/ui/StepsIndicators/StepsIndicators";

const ONBOARDING_STEPS = [
  { id: "goal", labelId: "onboardingFlow.progress.goal" },
  { id: "data", labelId: "onboardingFlow.progress.data" },
  { id: "source", labelId: "onboardingFlow.progress.source" },
  { id: "destination", labelId: "onboardingFlow.progress.destination" },
  { id: "connection", labelId: "onboardingFlow.progress.connection" },
  { id: "firstSync", labelId: "onboardingFlow.progress.firstSync" },
] as const;

/**
 * The AI agent path ends after the MCP install screen, because the agent sets up the connection, so its bar is shorter.
 */
const AI_AGENT_STEPS = [
  { id: "goal", labelId: "onboardingFlow.progress.goal" },
  { id: "data", labelId: "onboardingFlow.progress.data" },
  { id: "installMcp", labelId: "onboardingFlow.progress.installMcp" },
] as const;

export type OnboardingStepId = (typeof ONBOARDING_STEPS)[number]["id"] | (typeof AI_AGENT_STEPS)[number]["id"];

interface OnboardingStep {
  id: OnboardingStepId;
  labelId: string;
}

interface OnboardingProgressStep extends OnboardingStep {
  state: StepStatus;
}

/**
 * Only the AI agent path has the Install MCP step, so that step selects the shorter bar.
 */
const getStepList = (currentStepId: OnboardingStepId): readonly OnboardingStep[] =>
  currentStepId === "installMcp" ? AI_AGENT_STEPS : ONBOARDING_STEPS;

const getStepState = (stepIndex: number, currentStepIndex: number): StepStatus => {
  if (stepIndex === currentStepIndex) {
    return StepStatus.ACTIVE;
  }
  return stepIndex < currentStepIndex ? StepStatus.COMPLETE : StepStatus.INCOMPLETE;
};

export const getOnboardingProgress = (currentStepId: OnboardingStepId): OnboardingProgressStep[] => {
  const steps = getStepList(currentStepId);
  const currentStepIndex = steps.findIndex(({ id }) => id === currentStepId);
  return steps.map(({ id, labelId }, stepIndex) => ({ id, labelId, state: getStepState(stepIndex, currentStepIndex) }));
};
