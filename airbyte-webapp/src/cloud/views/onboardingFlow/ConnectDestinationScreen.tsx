import React from "react";

import { ConnectorCardValues } from "area/connector/components/ConnectorForm";
import {
  useDestinationDefinition,
  useDestinationDefinitionVersion,
  useGetDestinationDefinitionSpecification,
  useInvalidateFirstDestinationPage,
  useUpdateDestination,
} from "core/api";
import { DestinationRead } from "core/api/types/AirbyteClient";

import { ConnectorStepActions, ConnectorStepForm } from "./ConnectorStepForm";
import { MissingConnectorStep } from "./ConnectorStepLayout";
import { getStepDestination } from "./draftConnectors";
import { OnboardingActionBar } from "./OnboardingActionBar";
import { resolveDestinationDefinitionId } from "./onboardingConnectors";
import { useDestinationLists } from "./useConnectorLists";
import { useSaveOnboardingStatus } from "./useSaveOnboardingStatus";

/** The ready destination of the choice first, else the one draft destination that the destination screen keeps. */
const useStepDestination = (destinationChoice?: string) => {
  const { drafts, readyConnectors: readyDestinations } = useDestinationLists();
  const chosenDefinitionId = destinationChoice ? resolveDestinationDefinitionId(destinationChoice) : undefined;
  const isChosen = ({ destinationDefinitionId }: DestinationRead) => destinationDefinitionId === chosenDefinitionId;

  return getStepDestination(readyDestinations, drafts.at(0), isChosen);
};

interface DestinationStepProps extends ConnectorStepActions {
  destination: DestinationRead;
}

const DestinationStep: React.FC<DestinationStepProps> = ({ destination, ...stepActions }) => {
  const definition = useDestinationDefinition(destination.destinationDefinitionId);
  const specification = useGetDestinationDefinitionSpecification(destination.destinationId);
  const { supportLevel } = useDestinationDefinitionVersion(destination.destinationId);
  const { mutateAsync: updateDestination } = useUpdateDestination();
  const refreshDestinations = useInvalidateFirstDestinationPage();

  const saveDestination = async (values: ConnectorCardValues) => {
    const savedDestination = await updateDestination({ values, destinationId: destination.destinationId });
    await refreshDestinations();
    return savedDestination;
  };

  return (
    <ConnectorStepForm
      formType="destination"
      connector={destination}
      definition={definition}
      specification={specification}
      supportLevel={supportLevel}
      saveConnector={saveDestination}
      refreshConnectors={refreshDestinations}
      {...stepActions}
    />
  );
};

interface ConnectDestinationScreenProps {
  destinationChoice?: string;
  onBack: () => void;
  onContinue: () => void;
  onSkip?: () => void;
}

/** Back returns to the source, and a set-up destination moves on to the connection. Each saves its status first. */
export const ConnectDestinationScreen: React.FC<ConnectDestinationScreenProps> = ({
  destinationChoice,
  onBack,
  onContinue,
  onSkip,
}) => {
  const destination = useStepDestination(destinationChoice);
  const { saveStatus, isSaving } = useSaveOnboardingStatus();
  const goBack = () => saveStatus("create_source", onBack);
  const goToConnection = () => saveStatus("create_connection", onContinue);

  if (!destination) {
    return (
      <MissingConnectorStep
        connectorType="destination"
        actions={<OnboardingActionBar onSkip={onSkip} onBack={goBack} isSaving={isSaving} />}
      />
    );
  }
  return (
    <DestinationStep
      destination={destination}
      onSkip={onSkip}
      onBack={goBack}
      onSetUp={goToConnection}
      isSavingStatus={isSaving}
    />
  );
};
