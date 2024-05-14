import React, { useEffect } from "react";
import { useFormContext, useFormState } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Tooltip } from "components/ui/Tooltip";

import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useFormChangeTrackerService } from "hooks/services/FormChangeTracker";

import { FormConnectionFormValues } from "../../formConfig";
import { ResponseMessage } from "../../ResponseMessage";

interface TableControlsProps {
  isAllRowsExpanded: boolean;
  toggleAllRowsExpanded: (expanded: boolean) => void;
}

export const TableControls: React.FC<TableControlsProps> = ({ isAllRowsExpanded, toggleAllRowsExpanded }) => {
  const { mode, getErrorMessage, refreshSchema } = useConnectionFormService();
  const { discardRefreshedSchema, schemaRefreshing } = useConnectionEditService();
  const { schemaHasBeenRefreshed } = useConnectionEditService();
  const { isValid, isDirty, isSubmitting, isSubmitSuccessful, errors } = useFormState<FormConnectionFormValues>();
  const { reset, trigger } = useFormContext<FormConnectionFormValues>();

  // for discard and submit buttons
  const isControlDisabled = isSubmitting || (!isDirty && !schemaHasBeenRefreshed);
  const isSubmitDisabled = !isValid || mode === "readonly";
  const errorMessage = isDirty && getErrorMessage(isValid, errors);
  const successMessage = isSubmitSuccessful && !isDirty && <FormattedMessage id="form.changesSaved" />;
  const { trackFormChange, clearFormChange } = useFormChangeTrackerService();

  useEffect(() => {
    // <Form /> component remounts with <FormChangeTracker /> on schema refresh,
    // so we need to start tracking changes again manually
    if (schemaHasBeenRefreshed) {
      // force validation of the form after schema refresh to highlight errors
      trigger("syncCatalog.streams");
      trackFormChange("schemaHasBeenRefreshed", true);
      return;
    }

    clearFormChange("schemaHasBeenRefreshed");
  }, [clearFormChange, schemaHasBeenRefreshed, trackFormChange, trigger]);

  const onCancelButtonClick = () => {
    reset();
    if (schemaHasBeenRefreshed) {
      clearFormChange("schemaHasBeenRefreshed");
    }
    discardRefreshedSchema();
  };

  return (
    <FlexContainer justifyContent="flex-end" alignItems="center" direction="row" gap="lg">
      {isDirty || schemaHasBeenRefreshed ? (
        <>
          <ResponseMessage dirty={isDirty} successMessage={successMessage} errorMessage={errorMessage} />
          <FlexContainer gap="md">
            <Button
              type="button"
              variant="secondary"
              disabled={isControlDisabled}
              onClick={onCancelButtonClick}
              data-testid="cancel-edit-button"
            >
              <FormattedMessage id="form.discardChanges" />
            </Button>
            <Button
              type="submit"
              isLoading={isSubmitting}
              disabled={isControlDisabled || isSubmitDisabled}
              data-testid="save-edit-button"
            >
              <FormattedMessage id="form.saveChanges" />
            </Button>
          </FlexContainer>
        </>
      ) : (
        <>
          <Tooltip
            placement="top"
            control={<Button variant="secondary" icon="rotate" onClick={refreshSchema} disabled={schemaRefreshing} />}
          >
            <FormattedMessage id="connection.updateSchema" />
          </Tooltip>
          <Tooltip
            placement="top"
            control={
              <Button
                icon={isAllRowsExpanded ? "collapseAll" : "expandAll"}
                variant="secondary"
                type="button"
                onClick={() => toggleAllRowsExpanded(!isAllRowsExpanded)}
              />
            }
          >
            <FormattedMessage id={isAllRowsExpanded ? "tables.collapseAll" : "tables.expandAll"} />
          </Tooltip>
        </>
      )}
    </FlexContainer>
  );
};
