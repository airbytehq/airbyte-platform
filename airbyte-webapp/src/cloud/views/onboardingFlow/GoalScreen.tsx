import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { RadioButtonTileOption } from "area/connection/components/CreateConnection/RadioButtonTile";
import { RadioButtonTiles } from "area/connection/components/CreateConnection/RadioButtonTiles";

import { OnboardingActionBar } from "./OnboardingActionBar";
import { OnboardingScreenLayout } from "./OnboardingScreenLayout";
import { useTrackOnboardingAnswer } from "./useTrackOnboardingAnswer";

export type OnboardingGoal = "centralize" | "aiAgents" | "replicateDatabase" | "exploring";

/** The goals of the selected-state Figma frame 5:183249. The goal does not change the path. */
const GOAL_OPTIONS: Array<RadioButtonTileOption<OnboardingGoal>> = [
  {
    value: "centralize",
    label: <FormattedMessage id="onboardingFlow.goal.centralize" />,
    description: <FormattedMessage id="onboardingFlow.goal.centralize.description" />,
  },
  {
    value: "aiAgents",
    label: <FormattedMessage id="onboardingFlow.goal.aiAgents" />,
    description: <FormattedMessage id="onboardingFlow.goal.aiAgents.description" />,
  },
  {
    value: "replicateDatabase",
    label: <FormattedMessage id="onboardingFlow.goal.replicateDatabase" />,
    description: <FormattedMessage id="onboardingFlow.goal.replicateDatabase.description" />,
  },
  {
    value: "exploring",
    label: <FormattedMessage id="onboardingFlow.goal.exploring" />,
    description: <FormattedMessage id="onboardingFlow.goal.exploring.description" />,
  },
];

interface GoalScreenProps {
  goal?: OnboardingGoal;
  onGoalChange: (goal: OnboardingGoal) => void;
  onContinue: () => void;
  onSkip?: () => void;
}

export const GoalScreen: React.FC<GoalScreenProps> = ({ goal, onGoalChange, onContinue, onSkip }) => {
  const { formatMessage } = useIntl();
  const trackAnswer = useTrackOnboardingAnswer();

  const continueWithGoal = (selectedGoal: OnboardingGoal) => {
    trackAnswer("goal", { goal: selectedGoal });
    onContinue();
  };

  return (
    <OnboardingScreenLayout
      title={<FormattedMessage id="onboardingFlow.goal.title" />}
      actions={<OnboardingActionBar onSkip={onSkip} onContinue={goal ? () => continueWithGoal(goal) : undefined} />}
    >
      <RadioButtonTiles
        name="onboardingGoal"
        direction="column"
        options={GOAL_OPTIONS}
        selectedValue={goal ?? ""}
        onSelectRadioButton={onGoalChange}
        aria-label={formatMessage({ id: "onboardingFlow.goal.title" })}
      />
    </OnboardingScreenLayout>
  );
};
