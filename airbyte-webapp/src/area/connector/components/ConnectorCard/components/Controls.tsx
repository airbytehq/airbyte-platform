import React, { useId, useState } from "react";
import { createPortal } from "react-dom";
import { useFormContext } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { DropdownButton } from "components/ui/DropdownButton";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Tooltip } from "components/ui/Tooltip";

import { useFormFooterSlot } from "area/connector/components/ConnectorDocumentationLayout/FormFooterSlotContext";
import { FailureReason, JobConfigType, LogEvents, LogRead } from "core/api/types/AirbyteClient";
import { useExperiment } from "core/services/Experiment";

import styles from "./Controls.module.scss";
import { TestCard } from "./TestCard";

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
  /** The `id` of the `<form>` element. The submit button needs it when it renders in the footer slot, outside the form. */
  formElementId: string;
  canEdit: boolean;
}

const MAX_LISTED_REQUIRED_FIELDS = 3;

/** Returns undefined when no required field is empty. */
const useFillRequiredFieldsMessage = (emptyRequiredFields: string[]) => {
  const { formatMessage, formatList } = useIntl();
  if (emptyRequiredFields.length === 0) {
    return undefined;
  }

  const hiddenCount = emptyRequiredFields.length - MAX_LISTED_REQUIRED_FIELDS;
  const listedFields =
    hiddenCount > 0
      ? [
          ...emptyRequiredFields.slice(0, MAX_LISTED_REQUIRED_FIELDS),
          formatMessage({ id: "connectorForm.fillRequiredFields.more" }, { count: hiddenCount }),
        ]
      : emptyRequiredFields;
  return {
    tooltip: (
      <FormattedMessage
        id="connectorForm.fillRequiredFields"
        values={{
          fields: (
            <ul className={styles.requiredFieldList}>
              {listedFields.map((field) => (
                <li key={field}>{field}</li>
              ))}
            </ul>
          ),
        }}
      />
    ),
    description: formatMessage(
      { id: "connectorForm.fillRequiredFields" },
      { fields: formatList(listedFields, { type: "conjunction" }) }
    ),
  };
};

/**
 * Renders in the form footer slot of `ConnectorDocumentationLayout` when there is one, else in place.
 * In the slot, the buttons are outside the form's disabled fieldset, so they get their own.
 */
const FooterSlotButtonRow: React.FC<React.PropsWithChildren<{ canEdit: boolean }>> = ({ canEdit, children }) => {
  const footerSlot = useFormFooterSlot();
  const row = <FlexContainer>{children}</FlexContainer>;

  return footerSlot
    ? createPortal(
        <fieldset disabled={!canEdit} className={styles.footer}>
          {row}
        </fieldset>,
        footerSlot
      )
    : row;
};

export const Controls: React.FC<IProps> = ({
  isTestConnectionInProgress,
  isSubmitting,
  formType,
  hasDefinition,
  isEditMode,
  isDraftMode = false,
  isValid,
  emptyRequiredFields,
  dirty,
  onDeleteClick,
  onCancelClick,
  leftSlot = null,
  onSubmitWithoutCheck,
  onSaveDraft,
  formElementId,
  canEdit,
  ...restProps
}) => {
  const { formatMessage } = useIntl();
  const { trigger } = useFormContext();
  const showTestCard =
    hasDefinition &&
    (isEditMode || isTestConnectionInProgress || restProps.connectionTestSuccess || restProps.errorMessage);
  const [isSubmittingWithoutCheck, setIsSubmittingWithoutCheck] = useState(false);
  const [isSavingDraft, setIsSavingDraft] = useState(false);
  const shouldAllowSavingWithoutTesting = useExperiment("connector.allowSavingWithoutTesting");
  const fillRequiredFieldsMessage = useFillRequiredFieldsMessage(emptyRequiredFields);
  const submitButtonDescribedBy = useId();
  const submitBlocked = isSubmitting || fillRequiredFieldsMessage !== undefined;

  const submitButton =
    onSubmitWithoutCheck && shouldAllowSavingWithoutTesting ? (
      <DropdownButton
        type="submit"
        form={formElementId}
        data-testid={`${isEditMode ? "edit" : "create"}-${formType}-button`}
        isLoading={isSubmitting || isSubmittingWithoutCheck}
        disabled={submitBlocked || (isEditMode && !dirty)}
        aria-describedby={fillRequiredFieldsMessage && submitButtonDescribedBy}
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
        <FormattedMessage id="connectorForm.saveAndTest" />
      </DropdownButton>
    ) : (
      <Button
        type="submit"
        form={formElementId}
        data-testid={`${isEditMode ? "edit" : "create"}-${formType}-button`}
        isLoading={isSubmitting}
        disabled={submitBlocked || (isEditMode && !isDraftMode && !dirty)}
        aria-describedby={fillRequiredFieldsMessage && submitButtonDescribedBy}
      >
        <FormattedMessage id="connectorForm.saveAndTest" />
      </Button>
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
      <FooterSlotButtonRow canEdit={canEdit}>
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
        <Tooltip control={submitButton} disabled={!fillRequiredFieldsMessage} placement="top" cursor="not-allowed">
          {fillRequiredFieldsMessage?.tooltip}
        </Tooltip>
        {/* The screen-reader text for the disabled submit button, which cannot take focus to show the tooltip */}
        {fillRequiredFieldsMessage && (
          <span id={submitButtonDescribedBy} className={styles.visuallyHidden}>
            {fillRequiredFieldsMessage.description}
          </span>
        )}
      </FooterSlotButtonRow>
    </>
  );
};
