import React, { useState } from "react";
import { useFormContext } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { CopyButton } from "components/ui/CopyButton";
import { DropdownButton } from "components/ui/DropdownButton";
import { FlexContainer, FlexItem } from "components/ui/Flex";

import { maskSecrets } from "area/connector/utils/maskSecrets";
import { SynchronousJobRead } from "core/api/types/AirbyteClient";
import { useExperiment } from "hooks/services/Experiment";

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
  job?: SynchronousJobRead;
  connectionTestSuccess: boolean;
  hasDefinition: boolean;
  isEditMode: boolean;
  leftSlot?: React.ReactNode;
  onSubmitWithoutCheck?: () => Promise<void>;
  onCopyConfig?: () => {
    config: Record<string, unknown>;
    schema: unknown;
    name: string;
    workspaceId: string;
    definitionId: string;
  };
}

export const Controls: React.FC<IProps> = ({
  isTestConnectionInProgress,
  isSubmitting,
  formType,
  hasDefinition,
  isEditMode,
  isValid,
  dirty,
  onDeleteClick,
  onCancelClick,
  leftSlot = null,
  onSubmitWithoutCheck,
  onCopyConfig,
  ...restProps
}) => {
  const { formatMessage } = useIntl();
  const { trigger } = useFormContext();
  const showTestCard =
    hasDefinition &&
    (isEditMode || isTestConnectionInProgress || restProps.connectionTestSuccess || restProps.errorMessage);
  const buttonContent = isEditMode ? (
    <FormattedMessage id="form.saveChangesAndTest" />
  ) : (
    <FormattedMessage id={`onboarding.${formType}SetUp.buttonText`} />
  );
  const [isSubmittingWithoutCheck, setIsSubmittingWithoutCheck] = useState(false);
  const shouldAllowSavingWithoutTesting = useExperiment("connector.allowSavingWithoutTesting");

  return (
    <>
      {showTestCard && (
        <TestCard
          {...restProps}
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
        {onCopyConfig && (
          <CopyButton
            content={() => {
              const { config, schema, name, workspaceId, definitionId } = onCopyConfig();
              // Mask secrets if schema is available
              const maskedConfig =
                schema && typeof schema === "object" ? maskSecrets(config, schema as Record<string, unknown>) : config;
              // Create the expanded JSON structure
              const jsonData = {
                name,
                workspaceId,
                definitionId,
                configuration: maskedConfig,
              };
              return JSON.stringify(jsonData, null, 2);
            }}
            variant="secondary"
          >
            <FormattedMessage id="connectorForm.copyConfig" />
          </CopyButton>
        )}

        {isEditMode && (
          <Button type="button" variant="secondary" disabled={isSubmitting || !dirty} onClick={onCancelClick}>
            <FormattedMessage id="form.cancel" />
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
            disabled={isSubmitting || (isEditMode && !dirty)}
          >
            {buttonContent}
          </Button>
        )}
      </FlexContainer>
    </>
  );
};
