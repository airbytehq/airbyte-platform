import { OrganizationOnboardingStatus } from "core/api/types/AirbyteClient";

import { getInitialScreen } from "./onboardingScreenSteps";

describe("getInitialScreen", () => {
  it("opens the goal screen when the organization has no draft source", () => {
    expect(getInitialScreen("survey", false)).toBe("goal");
  });

  it("opens the destination screen when the sources were saved", () => {
    expect(getInitialScreen("survey", true)).toBe("destination");
  });

  it.each<OrganizationOnboardingStatus>(["choose_method", "create_source", "run_first_sync"])(
    "opens the destination screen at %s",
    (status) => {
      expect(getInitialScreen(status, false)).toBe("destination");
    }
  );
});
