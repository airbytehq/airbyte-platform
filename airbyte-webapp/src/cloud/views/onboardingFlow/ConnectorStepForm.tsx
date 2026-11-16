import { useState } from "react";

import { ConnectorCard } from "area/connector/components/ConnectorCard";
import { ConnectorCardValues } from "area/connector/components/ConnectorForm";
import { SupportLevel } from "core/api/types/AirbyteClient";
import {
  ConnectorDefinition,
  ConnectorDefinitionSpecificationRead,
  ConnectorSpecification,
  ConnectorT,
} from "core/domain/connector";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

import {
  CONNECTOR_STEP_FORM_ID,
  ConnectorFormStateReporter,
  ConnectorStepActionBar,
  INITIAL_CONNECTOR_FORM_STATE,
} from "./ConnectorStepActionBar";
import { ConnectorStepLayout } from "./ConnectorStepLayout";

export interface ConnectorStepActions {
  onSkip?: () => void;
  onBack: () => void;
  onSetUp: () => void;
  isSavingStatus: boolean;
}

interface ConnectorStepFormProps<T extends ConnectorT> extends ConnectorStepActions {
  formType: "source" | "destination";
  connector: T;
  definition: ConnectorDefinition;
  specification: ConnectorDefinitionSpecificationRead;
  supportLevel?: SupportLevel;
  /** Saves the form values to the connector, then refreshes the connector list. */
  saveConnector: (values: ConnectorCardValues) => Promise<T>;
  refreshConnectors: () => Promise<void>;
}

/**
 * The form edits the draft, as the connector settings pages do. A passing test on the saved draft makes it a ready
 * connector on the server, and then `onDraftPromoted` runs. A connector that is already ready saves after a passing
 * test. Continue in the action bar submits the form, so the form has no buttons of its own.
 */
export const ConnectorStepForm = <T extends ConnectorT>({
  formType,
  connector,
  definition,
  specification,
  supportLevel,
  saveConnector,
  refreshConnectors,
  onSkip,
  onBack,
  onSetUp,
  isSavingStatus,
}: ConnectorStepFormProps<T>) => {
  const canEditConnector = useGeneratedIntent(
    formType === "source" ? Intent.CreateOrEditSource : Intent.CreateOrEditDestination
  );
  const [formState, setFormState] = useState(INITIAL_CONNECTOR_FORM_STATE);
  const [isTestConnectionInProgress, setIsTestConnectionInProgress] = useState(false);

  const saveAndFinishSetUp = async (values: ConnectorCardValues) => {
    await saveConnector(values);
    onSetUp();
  };

  const finishSetUp = async () => {
    await refreshConnectors();
    onSetUp();
  };

  return (
    <ConnectorStepLayout
      definition={definition}
      actions={
        <ConnectorStepActionBar
          isConnectorReady={!connector.isDraft}
          canEditConnector={canEditConnector}
          formState={formState}
          isTestConnectionInProgress={isTestConnectionInProgress}
          onSkip={onSkip}
          onBack={onBack}
          onContinue={onSetUp}
          isSavingStatus={isSavingStatus}
        />
      }
    >
      <ConnectorCard
        formType={formType}
        isEditMode
        connector={connector}
        formElementId={CONNECTOR_STEP_FORM_ID}
        hideFooterRow
        formAutomation={<ConnectorFormStateReporter onChange={setFormState} />}
        onTestConnectionInProgressChange={setIsTestConnectionInProgress}
        availableConnectorDefinitions={[definition]}
        selectedConnectorDefinitionSpecification={specification}
        selectedConnectorDefinitionId={ConnectorSpecification.id(specification)}
        supportLevel={supportLevel}
        reloadConfig={refreshConnectors}
        onSubmit={saveAndFinishSetUp}
        onSaveDraft={connector.isDraft ? saveConnector : undefined}
        onDraftPromoted={connector.isDraft ? finishSetUp : undefined}
      />
    </ConnectorStepLayout>
  );
};
