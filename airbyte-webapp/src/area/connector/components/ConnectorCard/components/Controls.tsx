import React, { useState } from "react";
import { useFormContext } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { DropdownButton } from "components/ui/DropdownButton";
import { FlexContainer, FlexItem } from "components/ui/Flex";

import { FailureReason, JobConfigType, LogEvents, LogRead } from "core/api/types/AirbyteClient";
import { useExperiment } from "core/services/Experiment";

import { TestCard } from "./TestCard";

interface IProps {
  formType: "source" | "destination";
  isSubmitting: boolean;
  isValid: boolean;
  dirty: boolean;
  onCancelClick: () => void;
  onDeleteClick?: () => void;
  onRetestClick: () => void;
  onCancelTesting: () => void;
  isTestConnectionInProgress?: boolean;
  errorMessage?: React.ReactNode;
  jobId?: string;
  jobConfigType?: JobConfigType;
  jobLogs?: LogEvents | LogRead;
  jobFailureReason?: FailureReason;
  connectionTestSuccess: boolean;
  hasDefinition: boolean;
  isEditMode: boolean;
  isDraftMode?: boolean;
  leftSlot?: React.ReactNode;
  onSubmitWithoutCheck?: () => Promise<void>;
  onSaveDraft?: () => Promise<void>;
}

export const Controls: React.FC<IProps> = ({
  isTestConnectionInProgress,
  isSubmitting,
  formType,
  hasDefinition,
  isEditMode,
  isDraftMode = false,
  isValid,
  dirty,
  onDeleteClick,
  onCancelClick,
  leftSlot = null,
  onSubmitWithoutCheck,
  onSaveDraft,
  ...restProps
}) => {
  const { formatMessage } = useIntl();
  const { trigger } = useFormContext();
  const showTestCard =
    hasDefinition &&
    (isEditMode || isTestConnectionInProgress || restProps.connectionTestSuccess || restProps.errorMessage);
  const buttonContent =
    isEditMode && !isDraftMode ? (
      <FormattedMessage id="form.saveChangesAndTest" />
    ) : (
      <FormattedMessage id={`onboarding.${formType}SetUp.buttonText`} />
    );
  const [isSubmittingWithoutCheck, setIsSubmittingWithoutCheck] = useState(false);
  const [isSavingDraft, setIsSavingDraft] = useState(false);
  const shouldAllowSavingWithoutTesting = useExperiment("connector.allowSavingWithoutTesting");

  return (
    <>
      {showTestCard && (
        <TestCard
          {...restProps}
          onRetestClick={isDraftMode ? undefined : restProps.onRetestClick}
          isValid={isValid}
          dirty={dirty}
          formType={formType}
          isTestConnectionInProgress={isTestConnectionInProgress}
          isEditMode={isEditMode}
        />
      )}
      <FlexContainer>
        {leftSlot}
        <FlexItem grow>
          {isEditMode && (
            <Button variant="danger" type="button" onClick={onDeleteClick} data-id="open-delete-modal">
              <FormattedMessage id={`tables.${formType}Delete`} />
            </Button>
          )}
        </FlexItem>
        {isEditMode && (
          <Button type="button" variant="secondary" disabled={isSubmitting || !dirty} onClick={onCancelClick}>
            <FormattedMessage id="form.cancel" />
          </Button>
        )}
        {onSaveDraft && (
          <Button
            type="button"
            variant="secondary"
            isLoading={isSavingDraft}
            disabled={isSubmitting || isSavingDraft}
            onClick={async () => {
              setIsSavingDraft(true);
              try {
                await onSaveDraft();
              } finally {
                setIsSavingDraft(false);
              }
            }}
          >
            <FormattedMessage id="connectorForm.saveDraft" />
          </Button>
        )}
        {onSubmitWithoutCheck && shouldAllowSavingWithoutTesting ? (
          <DropdownButton
            type="submit"
            data-testid={`${isEditMode ? "edit" : "create"}-${formType}-button`}
            isLoading={isSubmitting || isSubmittingWithoutCheck}
            disabled={isSubmitting || (isEditMode && !dirty)}
            dropdown={{
              options: [
                {
                  displayName: formatMessage({ id: "connectorForm.submitWithoutCheck" }),
                  value: "skipCheckConnection",
                },
              ],
              onSelect: (option) => {
                if (option.value === "skipCheckConnection") {
                  trigger().then(async (valid) => {
                    if (valid) {
                      setIsSubmittingWithoutCheck(true);
                      try {
                        await onSubmitWithoutCheck();
                      } finally {
                        setIsSubmittingWithoutCheck(false);
                      }
                    }
                  });
                }
              },
            }}
          >
            {buttonContent}
          </DropdownButton>
        ) : (
          <Button
            type="submit"
            data-testid={`${isEditMode ? "edit" : "create"}-${formType}-button`}
            isLoading={isSubmitting}
            disabled={isSubmitting || (isEditMode && !isDraftMode && !dirty)}
          >
            {buttonContent}
          </Button>
        )}
      </FlexContainer>
    </>
  );
};
