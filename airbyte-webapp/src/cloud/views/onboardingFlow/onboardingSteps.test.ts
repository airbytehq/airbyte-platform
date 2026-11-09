import { StepStatus } from "components/ui/StepsIndicators/StepsIndicators";

import { getOnboardingStepState } from "./onboardingSteps";

describe("getOnboardingStepState", () => {
  it("marks steps before the current step as complete", () => {
    expect(getOnboardingStepState("goal", "source")).toBe(StepStatus.COMPLETE);
  });

  it("marks the current step as active", () => {
    expect(getOnboardingStepState("source", "source")).toBe(StepStatus.ACTIVE);
  });

  it("marks steps after the current step as incomplete", () => {
    expect(getOnboardingStepState("firstSync", "source")).toBe(StepStatus.INCOMPLETE);
  });
});
