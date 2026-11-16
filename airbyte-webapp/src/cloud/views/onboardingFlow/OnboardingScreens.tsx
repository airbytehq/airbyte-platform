import React, { useState } from "react";

import { useCurrentOrganizationInfo } from "core/api";

import { ChooseMethodScreen } from "./ChooseMethodScreen";
import { ConnectDestinationScreen } from "./ConnectDestinationScreen";
import { ConnectSourceScreen } from "./ConnectSourceScreen";
import { CreateConnectionScreen } from "./CreateConnectionScreen";
import { DestinationScreen } from "./DestinationScreen";
import { getSourcePicks } from "./draftConnectors";
import { FirstSyncScreen } from "./FirstSyncScreen";
import { GoalScreen, OnboardingGoal } from "./GoalScreen";
import { getDestinationChoice } from "./onboardingConnectors";
import { getInitialScreen, OnboardingScreenId } from "./onboardingScreenSteps";
import { SourcesScreen } from "./SourcesScreen";
import { useDestinationLists, useSourceLists } from "./useConnectorLists";

/**
 * The picks live here, so Back shows them again. The drafts and the ready connectors are the saved picks, so they fill
 * the picks when a member opens the flow. The goal is not stored.
 */
export const useOnboardingScreenState = () => {
  const status = useCurrentOrganizationInfo()?.onboardingStatus;
  const { drafts: draftSources, readyConnectors: readySources } = useSourceLists();
  const { drafts: draftDestinations, readyConnectors: readyDestinations } = useDestinationLists();

  const [screen, setScreen] = useState(() => getInitialScreen(status, draftSources.length > 0));
  const [goal, setGoal] = useState<OnboardingGoal>();
  const [sourcePicks, setSourcePicks] = useState(() => getSourcePicks(draftSources, readySources));
  const [destinationChoice, setDestinationChoice] = useState(() =>
    getDestinationChoice(draftDestinations, readyDestinations)
  );

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
          onContinue={goTo("chooseMethod")}
          onSkip={onSkip}
        />
      );
    case "chooseMethod":
      return (
        <ChooseMethodScreen
          sourcePicks={state.sourcePicks}
          destinationChoice={state.destinationChoice}
          onBack={goTo("destination")}
          onWebAppChosen={goTo("connectSource")}
          onSkip={onSkip}
        />
      );
    case "connectSource":
      return (
        <ConnectSourceScreen
          sourcePicks={state.sourcePicks}
          onBack={goTo("chooseMethod")}
          onContinue={goTo("connectDestination")}
          onSkip={onSkip}
        />
      );
    case "connectDestination":
      return (
        <ConnectDestinationScreen
          destinationChoice={state.destinationChoice}
          onBack={goTo("connectSource")}
          onContinue={goTo("createConnection")}
          onSkip={onSkip}
        />
      );
    case "createConnection":
      return (
        <CreateConnectionScreen
          sourcePicks={state.sourcePicks}
          destinationChoice={state.destinationChoice}
          onBack={goTo("connectDestination")}
          onContinue={goTo("firstSync")}
          onSkip={onSkip}
        />
      );
    case "firstSync":
      return (
        <FirstSyncScreen
          sourcePicks={state.sourcePicks}
          destinationChoice={state.destinationChoice}
          onBack={goTo("createConnection")}
          onSkip={onSkip}
        />
      );
  }
};
