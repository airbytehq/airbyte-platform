import React, { useState } from "react";

import { useCurrentOrganizationInfo, useDraftDestinationList, useDraftSourceList } from "core/api";

import { DestinationScreen } from "./DestinationScreen";
import { getSourcePicksFromDrafts } from "./draftConnectors";
import { GoalScreen, OnboardingGoal } from "./GoalScreen";
import { getDestinationChoiceFromDrafts } from "./onboardingConnectors";
import { getInitialScreen, OnboardingScreenId } from "./onboardingScreenSteps";
import { SourcesScreen } from "./SourcesScreen";

/**
 * The picks live here, so Back shows them again. The drafts are the saved picks, so they fill the picks when a member
 * opens the flow. The goal is not stored.
 */
export const useOnboardingScreenState = () => {
  const status = useCurrentOrganizationInfo()?.onboardingStatus;
  const draftSources = useDraftSourceList();
  const draftDestinations = useDraftDestinationList();

  const [screen, setScreen] = useState(() => getInitialScreen(status, draftSources.length > 0));
  const [goal, setGoal] = useState<OnboardingGoal>();
  const [sourcePicks, setSourcePicks] = useState(() => getSourcePicksFromDrafts(draftSources));
  const [destinationChoice, setDestinationChoice] = useState(() => getDestinationChoiceFromDrafts(draftDestinations));

  return { screen, setScreen, goal, setGoal, sourcePicks, setSourcePicks, destinationChoice, setDestinationChoice };
};

type OnboardingScreenState = ReturnType<typeof useOnboardingScreenState>;

interface OnboardingScreensProps {
  state: OnboardingScreenState;
  onSkip?: () => void;
}

export const OnboardingScreens: React.FC<OnboardingScreensProps> = ({ state, onSkip }) => {
  const { screen, setScreen } = state;
  const goTo = (nextScreen: OnboardingScreenId) => () => setScreen(nextScreen);

  switch (screen) {
    case "goal":
      return <GoalScreen goal={state.goal} onGoalChange={state.setGoal} onContinue={goTo("sources")} onSkip={onSkip} />;
    case "sources":
      return (
        <SourcesScreen
          picks={state.sourcePicks}
          onPicksChange={state.setSourcePicks}
          onBack={goTo("goal")}
          onContinue={goTo("destination")}
          onSkip={onSkip}
        />
      );
    case "destination":
      return (
        <DestinationScreen
          choice={state.destinationChoice}
          onChoiceChange={state.setDestinationChoice}
          onBack={goTo("sources")}
          onContinue={goTo("destination")}
          onSkip={onSkip}
        />
      );
  }
};
