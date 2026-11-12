import { RadioGroup } from "@headlessui/react";
import { useMutation } from "@tanstack/react-query";
import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { RadioButtonTile, RadioButtonTileOption } from "area/connection/components/CreateConnection/RadioButtonTile";
import { ConnectorIcon } from "area/connector/components/ConnectorIcon";
import { useDestinationDefinitionList } from "core/api";
import { DestinationDefinitionRead } from "core/api/types/AirbyteClient";

import styles from "./DestinationScreen.module.scss";
import { OnboardingActionBar } from "./OnboardingActionBar";
import {
  DESTINATION_OPTIONS,
  NOT_SURE_DESTINATION,
  RECOMMENDED_DESTINATION_ID,
  resolveDestinationDefinitionId,
} from "./onboardingConnectors";
import { OnboardingScreenLayout } from "./OnboardingScreenLayout";
import { useSaveOnboardingStatus } from "./useSaveOnboardingStatus";
import { isMissingDefinitionError, useSyncDraftDestination } from "./useSyncDraftConnectors";
import { useTrackOnboardingAnswer } from "./useTrackOnboardingAnswer";

const RADIO_GROUP_NAME = "onboardingDestination";

const NOT_SURE_OPTION: RadioButtonTileOption<string> = {
  value: NOT_SURE_DESTINATION,
  label: <FormattedMessage id="onboardingFlow.destination.notSure" />,
  description: <FormattedMessage id="onboardingFlow.destination.notSure.description" />,
  endIcon: <Icon type="question" size="lg" color="disabled" />,
};

const getDestinationTiles = (
  destinationDefinitionMap: Map<string, DestinationDefinitionRead>
): Array<RadioButtonTileOption<string>> =>
  DESTINATION_OPTIONS.flatMap(({ destinationDefinitionId, descriptionId }) => {
    const definition = destinationDefinitionMap.get(destinationDefinitionId);
    if (!definition) {
      return [];
    }
    return [
      {
        value: destinationDefinitionId,
        label: definition.name,
        description: <FormattedMessage id={descriptionId} />,
        endIcon: <ConnectorIcon icon={definition.icon} />,
      },
    ];
  });

/** The answer event names the member's choice, so "Not sure yet" is not reported as the Google Sheets pick. */
const NOT_SURE_ANSWER = "not_sure";

const getDestinationAnswer = (destinationChoice: string) =>
  destinationChoice === NOT_SURE_DESTINATION ? NOT_SURE_ANSWER : destinationChoice;

/** Continue saves the destination draft first, then moves the organization to the path choice. */
const useSaveDestination = (onSaved: () => void) => {
  const syncDraftDestination = useSyncDraftDestination();
  const { saveStatus, isSaving: isSavingStatus } = useSaveOnboardingStatus();
  const trackAnswer = useTrackOnboardingAnswer();

  const saveDraft = (destinationChoice: string) =>
    syncDraftDestination(resolveDestinationDefinitionId(destinationChoice));

  const continueWithSavedChoice = (destinationChoice: string) => {
    trackAnswer("destination", { choice: getDestinationAnswer(destinationChoice) });
    onSaved();
  };

  const { mutate: saveDestination, isLoading: isSavingDraft } = useMutation(saveDraft, {
    onSuccess: (_result, destinationChoice) =>
      saveStatus("choose_method", () => continueWithSavedChoice(destinationChoice)),
    useErrorBoundary: isMissingDefinitionError,
  });

  return { saveDestination, isSaving: isSavingDraft || isSavingStatus };
};

const RecommendationCard: React.FC<{ definition?: DestinationDefinitionRead }> = ({ definition }) => {
  const { formatMessage } = useIntl();

  return (
    <Card title={formatMessage({ id: "onboardingFlow.destination.recommendation.title" })}>
      <FlexContainer direction="column" gap="lg">
        <FlexContainer alignItems="center" gap="lg">
          <ConnectorIcon icon={definition?.icon} />
          <Text size="lg">
            <FormattedMessage id="onboardingFlow.destination.recommendation.description" />
          </Text>
        </FlexContainer>
        <Text size="sm" color="red">
          <FormattedMessage id="onboardingFlow.destination.recommendation.warning" />
        </Text>
      </FlexContainer>
    </Card>
  );
};

interface DestinationScreenProps {
  choice?: string;
  onChoiceChange: (choice: string) => void;
  onBack: () => void;
  onContinue: () => void;
  onSkip?: () => void;
}

export const DestinationScreen: React.FC<DestinationScreenProps> = ({
  choice,
  onChoiceChange,
  onBack,
  onContinue,
  onSkip,
}) => {
  const { formatMessage } = useIntl();
  const { destinationDefinitionMap } = useDestinationDefinitionList();
  const { saveDestination, isSaving } = useSaveDestination(onContinue);

  return (
    <OnboardingScreenLayout
      title={<FormattedMessage id="onboardingFlow.destination.title" />}
      actions={
        <OnboardingActionBar
          onSkip={onSkip}
          onBack={onBack}
          onContinue={choice ? () => saveDestination(choice) : undefined}
          isSaving={isSaving}
        />
      }
    >
      <RadioGroup
        value={choice ?? ""}
        onChange={onChoiceChange}
        className={styles.choices}
        aria-label={formatMessage({ id: "onboardingFlow.destination.title" })}
      >
        <div className={styles.choices__destinations}>
          {getDestinationTiles(destinationDefinitionMap).map((option) => (
            <RadioButtonTile key={option.value} option={option} name={RADIO_GROUP_NAME} />
          ))}
        </div>
        {destinationDefinitionMap.has(RECOMMENDED_DESTINATION_ID) && (
          <RadioButtonTile option={NOT_SURE_OPTION} name={RADIO_GROUP_NAME} />
        )}
      </RadioGroup>
      {choice === NOT_SURE_DESTINATION && (
        <RecommendationCard definition={destinationDefinitionMap.get(RECOMMENDED_DESTINATION_ID)} />
      )}
    </OnboardingScreenLayout>
  );
};
