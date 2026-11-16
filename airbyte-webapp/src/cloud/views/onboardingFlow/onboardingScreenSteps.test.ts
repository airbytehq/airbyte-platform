import { OrganizationOnboardingStatus } from "core/api/types/AirbyteClient";

import { getInitialScreen, OnboardingScreenId } from "./onboardingScreenSteps";

describe("getInitialScreen", () => {
  it("opens the goal screen when the organization has no draft source", () => {
    expect(getInitialScreen("survey", false)).toBe("goal");
  });

  it("opens the destination screen when the sources were saved", () => {
    expect(getInitialScreen("survey", true)).toBe("destination");
  });

  it.each<[OrganizationOnboardingStatus | undefined, boolean, OnboardingScreenId]>([
    ["choose_method", true, "chooseMethod"],
    ["create_source", true, "connectSource"],
    ["create_destination", true, "connectDestination"],
    ["create_connection", true, "createConnection"],
    ["run_first_sync", true, "createConnection"],
    [undefined, false, "goal"],
    [undefined, true, "destination"],
    ["completed", false, "goal"],
    ["completed", true, "destination"],
    ["skipped", false, "goal"],
    ["skipped", true, "destination"],
  ])("opens the screen of %s (has draft sources: %s)", (status, hasDraftSources, screen) => {
    expect(getInitialScreen(status, hasDraftSources)).toBe(screen);
  });
});
