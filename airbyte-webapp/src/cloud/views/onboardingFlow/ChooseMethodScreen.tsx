import React, { Suspense, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { LoadingSkeleton } from "components/ui/LoadingSkeleton";
import { Text } from "components/ui/Text";

import { RadioButtonTileOption } from "area/connection/components/CreateConnection/RadioButtonTile";
import { RadioButtonTiles } from "area/connection/components/CreateConnection/RadioButtonTiles";
import { ConnectorIcon } from "area/connector/components/ConnectorIcon";
import { useDestinationDefinitionList } from "core/api";
import { DestinationDefinitionRead, SourceDefinitionRead } from "core/api/types/AirbyteClient";
import { DefaultErrorBoundary } from "core/errors";

import { OnboardingActionBar } from "./OnboardingActionBar";
import { resolveDestinationDefinitionId } from "./onboardingConnectors";
import { OnboardingScreenLayout } from "./OnboardingScreenLayout";
import { useSaveOnboardingStatus } from "./useSaveOnboardingStatus";
import { useSuggestedSource } from "./useSuggestedSource";

type OnboardingPath = "webApp";

const PATH_OPTIONS: Array<RadioButtonTileOption<OnboardingPath>> = [
  {
    value: "webApp",
    label: <FormattedMessage id="onboardingFlow.chooseMethod.webApp" />,
    description: <FormattedMessage id="onboardingFlow.chooseMethod.webApp.description" />,
  },
];

const ConnectorName: React.FC<{ definition: SourceDefinitionRead | DestinationDefinitionRead }> = ({ definition }) => (
  <FlexContainer alignItems="center" gap="xs">
    <ConnectorIcon icon={definition.icon} />
    <Text size="sm" color="grey500">
      {definition.name}
    </Text>
  </FlexContainer>
);

interface SuggestedPairProps {
  sourcePicks: string[];
  destinationChoice?: string;
}

/** The pair to set up first: the easiest picked source and the chosen destination. */
const SuggestedPair: React.FC<SuggestedPairProps> = ({ sourcePicks, destinationChoice }) => {
  const { formatMessage } = useIntl();
  const source = useSuggestedSource(sourcePicks);
  const { destinationDefinitionMap } = useDestinationDefinitionList();
  const destination = destinationChoice
    ? destinationDefinitionMap.get(resolveDestinationDefinitionId(destinationChoice))
    : undefined;

  if (!source || !destination) {
    return null;
  }
  return (
    <Card title={formatMessage({ id: "onboardingFlow.chooseMethod.pair.title" })}>
      <FlexContainer direction="column" alignItems="center" gap="xl">
        <FlexContainer alignItems="center" gap="xs">
          <ConnectorName definition={source} />
          <Icon type="arrowRight" size="sm" color="disabled" />
          <ConnectorName definition={destination} />
        </FlexContainer>
        <Text size="lg">
          <FormattedMessage
            id="onboardingFlow.chooseMethod.pair.description"
            values={{ source: source.name, destination: destination.name }}
          />
        </Text>
      </FlexContainer>
    </Card>
  );
};

interface ChooseMethodScreenProps {
  sourcePicks: string[];
  destinationChoice?: string;
  onBack: () => void;
  onWebAppChosen: () => void;
  onSkip?: () => void;
}

/** Continue saves the path as the onboarding status, and the web app path continues with the source setup. */
export const ChooseMethodScreen: React.FC<ChooseMethodScreenProps> = ({
  sourcePicks,
  destinationChoice,
  onBack,
  onWebAppChosen,
  onSkip,
}) => {
  const { formatMessage } = useIntl();
  const [path, setPath] = useState<OnboardingPath>();
  const { saveStatus, isSaving } = useSaveOnboardingStatus();
  const chooseWebApp = () => saveStatus("create_source", onWebAppChosen);

  return (
    <OnboardingScreenLayout
      title={<FormattedMessage id="onboardingFlow.chooseMethod.title" />}
      actions={
        <OnboardingActionBar
          onSkip={onSkip}
          onBack={onBack}
          onContinue={path ? chooseWebApp : undefined}
          isSaving={isSaving}
        />
      }
    >
      <RadioButtonTiles
        name="onboardingPath"
        direction="column"
        options={PATH_OPTIONS}
        selectedValue={path ?? ""}
        onSelectRadioButton={setPath}
        aria-label={formatMessage({ id: "onboardingFlow.chooseMethod.title" })}
      />
      {/* A failed spec request shows its error in place of the card, so the path choice stays usable. */}
      <DefaultErrorBoundary>
        <Suspense fallback={<LoadingSkeleton />}>
          <SuggestedPair sourcePicks={sourcePicks} destinationChoice={destinationChoice} />
        </Suspense>
      </DefaultErrorBoundary>
    </OnboardingScreenLayout>
  );
};
