import { StepStatus } from "components/ui/StepsIndicators/StepsIndicators";

import { getOnboardingProgress } from "./onboardingSteps";

describe("getOnboardingProgress", () => {
  it("marks steps before the current step complete, the current step active, and later steps incomplete", () => {
    expect(getOnboardingProgress("source")).toEqual([
      { id: "goal", labelId: "onboardingFlow.progress.goal", state: StepStatus.COMPLETE },
      { id: "data", labelId: "onboardingFlow.progress.data", state: StepStatus.COMPLETE },
      { id: "source", labelId: "onboardingFlow.progress.source", state: StepStatus.ACTIVE },
      { id: "destination", labelId: "onboardingFlow.progress.destination", state: StepStatus.INCOMPLETE },
      { id: "connection", labelId: "onboardingFlow.progress.connection", state: StepStatus.INCOMPLETE },
      { id: "firstSync", labelId: "onboardingFlow.progress.firstSync", state: StepStatus.INCOMPLETE },
    ]);
  });

  it("shows the shorter AI agent bar with Install MCP as the current step", () => {
    expect(getOnboardingProgress("installMcp")).toEqual([
      { id: "goal", labelId: "onboardingFlow.progress.goal", state: StepStatus.COMPLETE },
      { id: "data", labelId: "onboardingFlow.progress.data", state: StepStatus.COMPLETE },
      { id: "installMcp", labelId: "onboardingFlow.progress.installMcp", state: StepStatus.ACTIVE },
    ]);
  });
});
