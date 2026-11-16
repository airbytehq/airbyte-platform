import { OrganizationOnboardingStatus } from "core/api/types/AirbyteClient";

import { OnboardingStepId } from "./onboardingSteps";

export type OnboardingScreenId =
  | "goal"
  | "sources"
  | "destination"
  | "chooseMethod"
  | "connectSource"
  | "connectDestination"
  | "createConnection"
  | "firstSync";

/**
 * The Figma frames show "Your data" on both the source and the destination screen, and "Connect source" on the path
 * choice screen and the source setup screen.
 */
export const PROGRESS_STEP_BY_SCREEN: Record<OnboardingScreenId, OnboardingStepId> = {
  goal: "goal",
  sources: "data",
  destination: "data",
  chooseMethod: "source",
  connectSource: "source",
  connectDestination: "destination",
  createConnection: "connection",
  firstSync: "firstSync",
};

/**
 * The connector setup screens show the documentation panel next to the form, and the connection screen scrolls its
 * stream table itself, so they fill the page body.
 */
export const FULL_WIDTH_SCREENS: ReadonlySet<OnboardingScreenId> = new Set([
  "connectSource",
  "connectDestination",
  "createConnection",
]);

type SetupStatus = Exclude<OrganizationOnboardingStatus, "survey" | "completed" | "skipped">;

const SCREEN_BY_SETUP_STATUS: Record<SetupStatus, OnboardingScreenId> = {
  choose_method: "chooseMethod",
  create_source: "connectSource",
  create_destination: "connectDestination",
  create_connection: "createConnection",
  run_first_sync: "firstSync",
};

const isSetupStatus = (status?: OrganizationOnboardingStatus): status is SetupStatus =>
  status !== undefined && status in SCREEN_BY_SETUP_STATUS;

/**
 * Picks the screen for a member who opens the flow. Each status after the survey has its own screen. The goal is not
 * stored, and draft sources exist only after Continue on the source screen, so they show that the member reached the
 * destination screen.
 */
export const getInitialScreen = (
  status: OrganizationOnboardingStatus | undefined,
  hasDraftSources: boolean
): OnboardingScreenId => {
  if (isSetupStatus(status)) {
    return SCREEN_BY_SETUP_STATUS[status];
  }
  return hasDraftSources ? "destination" : "goal";
};
