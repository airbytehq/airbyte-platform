import { OrganizationOnboardingStatus } from "core/api/types/AirbyteClient";

import { OnboardingStepId } from "./onboardingSteps";

export type OnboardingScreenId = "goal" | "sources" | "destination";

/**
 * The Figma frames show "Your data" on both the source and the destination screen.
 */
export const PROGRESS_STEP_BY_SCREEN: Record<OnboardingScreenId, OnboardingStepId> = {
  goal: "goal",
  sources: "data",
  destination: "data",
};

/**
 * Picks the screen for a member who opens the flow. The goal is not stored, and draft sources exist only after
 * Continue on the source screen, so they show that the member reached the destination screen.
 */
export const getInitialScreen = (
  status: OrganizationOnboardingStatus | undefined,
  hasDraftSources: boolean
): OnboardingScreenId => {
  if (status !== "survey") {
    return "destination";
  }
  return hasDraftSources ? "destination" : "goal";
};
