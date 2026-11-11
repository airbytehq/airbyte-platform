import React, { useId, useState } from "react";
import { createPortal } from "react-dom";
import { useFormContext } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { DropdownButton } from "components/ui/DropdownButton";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Tooltip } from "components/ui/Tooltip";

import { useFormFooterSlot } from "area/connector/components/ConnectorDocumentationLayout/FormFooterSlotContext";
import {
  SectionProgress,
  useSectionProgress,
} from "area/connector/components/ConnectorForm/components/Steps/SectionProgress";
import { useFillRequiredFieldsMessage } from "area/connector/components/ConnectorForm/requiredFields";
import { FailureReason, JobConfigType, LogEvents, LogRead } from "core/api/types/AirbyteClient";
import { useExperiment } from "core/services/Experiment";

import styles from "./Controls.module.scss";
import { TestCard } from "./TestCard";
import { FailedStep, TestFailurePanel } from "./TestFailurePanel";

interface IProps {
  formType: "source" | "destination";
  isSubmitting: boolean;
  isValid: boolean;
  /**
   * Labels of the required fields that are empty. Only `required` errors count: pattern, range and type errors are
   * ignored. Empty fields that OAuth fills show as one entry that asks the user to authenticate.
   */
  emptyRequiredFields: string[];
  dirty: boolean;
  onCancelClick: () => void;
  onDeleteClick?: () => void;
  onRetestClick: () => void;
  onCancelTesting: () => void;
  isTestConnectionInProgress?: boolean;
  /** The message of the last failed submit or retest. While it is set, the footer shows the failure panel. */
  errorMessage?: React.ReactNode;
  failedStep?: FailedStep;
  onDismissTestFailure: () => void;
  jobId?: string;
  jobConfigType?: JobConfigType;
  jobLogs?: LogEvents | LogRead;
  jobFailureReason?: FailureReason;
  connectionTestSuccess: boolean;
  hasDefinition: boolean;
  isEditMode: boolean;
  isDraftMode?: boolean;
  skipsConnectionCheck?: boolean;
  leftSlot?: React.ReactNode;
  onSubmitWithoutCheck?: () => Promise<void>;
  isSavingDraft?: boolean;
  onSaveDraft?: () => Promise<void>;
  /** The `id` of the `<form>` element. The submit button needs it when it renders in the footer slot, outside the form. */
  formElementId: string;
  canEdit: boolean;
}

/**
 * Renders in the form footer slot of `ConnectorDocumentationLayout` when there is one, else in place.
 * In the slot, the footer is outside the form's disabled fieldset, so it gets its own.
 */
const ControlsFooter: React.FC<React.PropsWithChildren<{ canEdit: boolean; testFailurePanel: React.ReactNode }>> = ({
  canEdit,
  testFailurePanel,
  children,
}) => {
  const footerSlot = useFormFooterSlot();
  const footer = (
    <FlexContainer direction="column">
      {testFailurePanel}
      <FlexContainer>{children}</FlexContainer>
    </FlexContainer>
  );

  return footerSlot
    ? createPortal(
        <fieldset disabled={!canEdit} className={styles.footer}>
          {footer}
        </fieldset>,
        footerSlot
      )
    : footer;
};

export const Controls: React.FC<IProps> = ({
  isTestConnectionInProgress,
  isSubmitting,
  formType,
  hasDefinition,
  isEditMode,
  isDraftMode = false,
  skipsConnectionCheck = false,
  isValid,
  emptyRequiredFields,
  dirty,
  onDeleteClick,
  onCancelClick,
  leftSlot = null,
  onSubmitWithoutCheck,
  isSavingDraft = false,
  onSaveDraft,
  formElementId,
  canEdit,
  errorMessage,
  failedStep = "test",
  jobId,
  jobConfigType,
  jobLogs,
  jobFailureReason,
  onDismissTestFailure,
  ...restProps
}) => {
  const { formatMessage } = useIntl();
  const { trigger } = useFormContext();
  const hasTestCardContent = isEditMode || isTestConnectionInProgress || restProps.connectionTestSuccess;
  const showTestCard = hasDefinition && hasTestCardContent;
  const hasFailure = Boolean(errorMessage);
  const hasTestFailure = hasFailure && failedStep === "test";
  // The edit-mode retest checks the saved connector, so an unchanged edit form has nothing new to submit
  const isUnchangedEdit = isEditMode && !isDraftMode && !dirty;
  const submitLabelId = hasTestFailure && !isUnchangedEdit ? "connectorForm.retryTest" : "connectorForm.saveAndTest";
  const [isSubmittingWithoutCheck, setIsSubmittingWithoutCheck] = useState(false);
  const shouldAllowSavingWithoutTesting = useExperiment("connector.allowSavingWithoutTesting");
  const fillRequiredFieldsMessage = useFillRequiredFieldsMessage(emptyRequiredFields);
  const sectionProgress = useSectionProgress();
  const isBeforeLastSection =
    sectionProgress !== undefined && sectionProgress.activeSectionIndex < sectionProgress.sectionCount - 1;
  const disabledSubmitMessage = isBeforeLastSection
    ? {
        tooltip: <FormattedMessage id="connectorForm.sections.completeAll" />,
        description: formatMessage({ id: "connectorForm.sections.completeAll" }),
      }
    : fillRequiredFieldsMessage;
  const submitButtonDescribedBy = useId();
  const submitBlocked = isSubmitting || disabledSubmitMessage !== undefined;

  const submitButton =
    onSubmitWithoutCheck && shouldAllowSavingWithoutTesting && !skipsConnectionCheck ? (
      <DropdownButton
        type="submit"
        form={formElementId}
        data-testid={`${isEditMode ? "edit" : "create"}-${formType}-button`}
        isLoading={isSubmitting || isSubmittingWithoutCheck}
        disabled={submitBlocked || (isEditMode && !dirty)}
        aria-describedby={disabledSubmitMessage && submitButtonDescribedBy}
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
        <FormattedMessage id={submitLabelId} />
      </DropdownButton>
    ) : (
      <Button
        type="submit"
        form={formElementId}
        data-testid={`${isEditMode ? "edit" : "create"}-${formType}-button`}
        isLoading={isSubmitting}
        disabled={submitBlocked || isUnchangedEdit}
        aria-describedby={disabledSubmitMessage && submitButtonDescribedBy}
      >
        <FormattedMessage id={skipsConnectionCheck ? "connectorForm.save" : submitLabelId} />
      </Button>
    );

  const testFailurePanel = hasFailure && (
    <TestFailurePanel
      failedStep={failedStep}
      fallbackMessage={errorMessage}
      jobId={jobId}
      jobConfigType={jobConfigType}
      jobLogs={jobLogs}
      failureReason={jobFailureReason}
      onClose={onDismissTestFailure}
    />
  );

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
      <ControlsFooter canEdit={canEdit} testFailurePanel={testFailurePanel}>
        {leftSlot}
        {sectionProgress && <SectionProgress {...sectionProgress} />}
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
            onClick={onSaveDraft}
          >
            <FormattedMessage id="connectorForm.saveDraft" />
          </Button>
        )}
        <Tooltip control={submitButton} disabled={!disabledSubmitMessage} placement="top" cursor="not-allowed">
          {disabledSubmitMessage?.tooltip}
        </Tooltip>
        {/* The screen-reader text for the disabled submit button, which cannot take focus to show the tooltip */}
        {disabledSubmitMessage && (
          <span id={submitButtonDescribedBy} className={styles.visuallyHidden}>
            {disabledSubmitMessage.description}
          </span>
        )}
      </ControlsFooter>
    </>
  );
};
