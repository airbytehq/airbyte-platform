import React, { useEffect } from "react";
import { useFormState } from "react-hook-form";

import { OnboardingActionBar } from "./OnboardingActionBar";

/** The connector form is the only form of a connector step, so the steps share one id. */
export const CONNECTOR_STEP_FORM_ID = "onboarding-connector-step-form";

export interface ConnectorFormState {
  isDirty: boolean;
  isSubmitting: boolean;
}

export const INITIAL_CONNECTOR_FORM_STATE: ConnectorFormState = { isDirty: false, isSubmitting: false };

/** Renders inside the connector form and passes its state to the action bar, which renders outside the form. */
export const ConnectorFormStateReporter: React.FC<{ onChange: (state: ConnectorFormState) => void }> = ({
  onChange,
}) => {
  const { isDirty, isSubmitting } = useFormState();

  useEffect(() => {
    onChange({ isDirty, isSubmitting });
  }, [isDirty, isSubmitting, onChange]);

  return null;
};

interface ConnectorStepActionBarProps {
  isConnectorReady: boolean;
  /** The member may edit the connector. Without it, Continue cannot submit the connector form. */
  canEditConnector: boolean;
  formState: ConnectorFormState;
  isTestConnectionInProgress: boolean;
  onSkip?: () => void;
  onBack: () => void;
  onContinue: () => void;
  isSavingStatus: boolean;
}

/**
 * Continue submits the connector form, so it saves and tests a draft or a changed connector, as Save & Test does on the
 * settings pages. A ready connector with no changes moves on without a new test. The buttons wait while any test runs,
 * including a retest from the test card.
 */
export const ConnectorStepActionBar: React.FC<ConnectorStepActionBarProps> = ({
  isConnectorReady,
  canEditConnector,
  formState,
  isTestConnectionInProgress,
  onSkip,
  onBack,
  onContinue,
  isSavingStatus,
}) => {
  const continuesWithoutTest = isConnectorReady && !formState.isDirty;
  const isBusy = isSavingStatus || formState.isSubmitting || isTestConnectionInProgress;
  const submitFormId = canEditConnector ? CONNECTOR_STEP_FORM_ID : undefined;

  return (
    <OnboardingActionBar
      onSkip={onSkip}
      onBack={onBack}
      onContinue={continuesWithoutTest ? onContinue : undefined}
      continueFormId={continuesWithoutTest ? undefined : submitFormId}
      isSaving={isBusy}
    />
  );
};
