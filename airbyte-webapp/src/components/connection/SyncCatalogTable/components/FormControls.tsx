import React, { useEffect } from "react";
import { useFormContext, useFormState } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";

import { useFormMode } from "core/services/ui/FormModeContext";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useFormChangeTrackerService } from "hooks/services/FormChangeTracker";

import { FormConnectionFormValues } from "../../ConnectionForm/formConfig";
import { ResponseMessage } from "../../ConnectionForm/ResponseMessage";

export const FormControls: React.FC<React.PropsWithChildren> = ({ children }) => {
  const { getErrorMessage } = useConnectionFormService();
  const { mode } = useFormMode();
  const { discardRefreshedSchema, schemaHasBeenRefreshed } = useConnectionEditService();
  const { isValid, isDirty, isSubmitting, isSubmitSuccessful, errors } = useFormState<FormConnectionFormValues>();
  const { reset, trigger } = useFormContext<FormConnectionFormValues>();

  const isSubmitDisabled = !isValid || mode === "readonly";
  const errorMessage = isDirty && getErrorMessage(isValid, errors);
  const successMessage = isSubmitSuccessful && !isDirty && <FormattedMessage id="form.changesSaved" />;
  const { trackFormChange, clearFormChange } = useFormChangeTrackerService();

  useEffect(() => {
    if (schemaHasBeenRefreshed) {
      /**
       * force validation of the form after schema refresh to highlight errors,
       * but we need to wait for the form to be filled with new values
       */
      setTimeout(() => trigger("syncCatalog.streams"), 0);
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
      <ResponseMessage dirty={isDirty} successMessage={successMessage} errorMessage={errorMessage} />
      {isDirty || schemaHasBeenRefreshed ? (
        <FlexContainer gap="md">
          <Button
            type="button"
            variant="secondary"
            disabled={isSubmitting}
            onClick={onCancelButtonClick}
            data-testid="cancel-edit-button"
          >
            <FormattedMessage id="form.discardChanges" />
          </Button>
          <Button type="submit" isLoading={isSubmitting} disabled={isSubmitDisabled} data-testid="save-edit-button">
            <FormattedMessage id="form.saveChanges" />
          </Button>
        </FlexContainer>
      ) : (
        children
      )}
    </FlexContainer>
  );
};
