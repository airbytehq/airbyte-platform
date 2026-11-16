import React from "react";

import { ConnectorCardValues } from "area/connector/components/ConnectorForm";
import {
  useGetSourceDefinitionSpecification,
  useInvalidateFirstSourcePage,
  useSourceDefinition,
  useSourceDefinitionVersion,
  useUpdateSource,
} from "core/api";
import { SourceRead } from "core/api/types/AirbyteClient";

import { ConnectorStepActions, ConnectorStepForm } from "./ConnectorStepForm";
import { MissingConnectorStep } from "./ConnectorStepLayout";
import { getPickedReadySource } from "./draftConnectors";
import { OnboardingActionBar } from "./OnboardingActionBar";
import { useSourceLists } from "./useConnectorLists";
import { useSaveOnboardingStatus } from "./useSaveOnboardingStatus";
import { useSuggestedSource } from "./useSuggestedSource";

/** The draft of the source that the `chooseMethod` screen suggests. */
const useSuggestedDraft = (sourcePicks: string[]) => {
  const suggestedSource = useSuggestedSource(sourcePicks);
  const { drafts } = useSourceLists();

  return drafts.find(({ sourceDefinitionId }) => sourceDefinitionId === suggestedSource?.sourceDefinitionId);
};

interface SourceStepProps extends ConnectorStepActions {
  source: SourceRead;
}

const SourceStep: React.FC<SourceStepProps> = ({ source, ...stepActions }) => {
  const definition = useSourceDefinition(source.sourceDefinitionId);
  const specification = useGetSourceDefinitionSpecification(source.sourceId);
  const { supportLevel } = useSourceDefinitionVersion(source.sourceId);
  const { mutateAsync: updateSource } = useUpdateSource();
  const refreshSources = useInvalidateFirstSourcePage();

  const saveSource = async (values: ConnectorCardValues) => {
    const savedSource = await updateSource({ values, sourceId: source.sourceId });
    await refreshSources();
    return savedSource;
  };

  return (
    <ConnectorStepForm
      formType="source"
      connector={source}
      definition={definition}
      specification={specification}
      supportLevel={supportLevel}
      saveConnector={saveSource}
      refreshConnectors={refreshSources}
      {...stepActions}
    />
  );
};

interface SuggestedDraftStepProps extends ConnectorStepActions {
  sourcePicks: string[];
}

const SuggestedDraftStep: React.FC<SuggestedDraftStepProps> = ({ sourcePicks, ...stepActions }) => {
  const draft = useSuggestedDraft(sourcePicks);

  if (!draft) {
    return (
      <MissingConnectorStep
        connectorType="source"
        actions={
          <OnboardingActionBar
            onSkip={stepActions.onSkip}
            onBack={stepActions.onBack}
            isSaving={stepActions.isSavingStatus}
          />
        }
      />
    );
  }
  return <SourceStep source={draft} {...stepActions} />;
};

interface ConnectSourceScreenProps {
  sourcePicks: string[];
  onBack: () => void;
  onContinue: () => void;
  onSkip?: () => void;
}

/**
 * Opens the ready source of a pick first, else the suggested draft, so the step opens the source that the path choice
 * suggests. Only the suggestion loads the specs of the picks, so a ready source opens without them. Back returns to the
 * path choice, and a set-up source moves on to the destination. Each saves its status first.
 */
export const ConnectSourceScreen: React.FC<ConnectSourceScreenProps> = ({
  sourcePicks,
  onBack,
  onContinue,
  onSkip,
}) => {
  const { readyConnectors: readySources } = useSourceLists();
  const { saveStatus, isSaving } = useSaveOnboardingStatus();
  const stepActions: ConnectorStepActions = {
    onSkip,
    onBack: () => saveStatus("choose_method", onBack),
    onSetUp: () => saveStatus("create_destination", onContinue),
    isSavingStatus: isSaving,
  };

  const readySource = getPickedReadySource(readySources, sourcePicks);
  if (readySource) {
    return <SourceStep source={readySource} {...stepActions} />;
  }
  return <SuggestedDraftStep sourcePicks={sourcePicks} {...stepActions} />;
};
