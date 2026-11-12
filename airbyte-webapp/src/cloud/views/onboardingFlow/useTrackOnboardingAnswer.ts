import { Action, Namespace, useAnalyticsService } from "core/services/analytics";

import { OnboardingScreenId } from "./onboardingScreenSteps";

/** Each screen sends its saved answer as one `Onboarding.Answered` event, keyed by the stable screen id. */
export const useTrackOnboardingAnswer = () => {
  const analyticsService = useAnalyticsService();

  return (screenId: OnboardingScreenId, answer: Record<string, unknown>) =>
    analyticsService.track(Namespace.ONBOARDING, Action.ANSWERED, {
      actionDescription: `Onboarding ${screenId} answered`,
      screen_id: screenId,
      ...answer,
    });
};
