import React, { useEffect } from "react";
import { useFormContext, useFormState } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";

import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useFormChangeTrackerService } from "hooks/services/FormChangeTracker";

import { FormConnectionFormValues } from "./formConfig";
import { ResponseMessage } from "./ResponseMessage";

interface UpdateConnectionFormControlsProps {
  onCancel: () => void;
}

/**
 * react-hook-form version of UpdateControls
 * the component is used on replications page for saving changes in connection configurations and sync streams
 */
export const UpdateConnectionFormControls: React.FC<UpdateConnectionFormControlsProps> = ({ onCancel }) => {
  const { mode, getErrorMessage } = useConnectionFormService();
  const { schemaHasBeenRefreshed } = useConnectionEditService();
  const { isValid, isDirty, isSubmitting, isSubmitSuccessful, errors } = useFormState<FormConnectionFormValues>();
  const { reset, trigger, formState } = useFormContext<FormConnectionFormValues>();

  // for cancel and submit buttons
  const isControlDisabled = isSubmitting || (!isDirty && !(schemaHasBeenRefreshed || isDirty));
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
    /**
     * Since we update streams via "setValue()" (mutable) instead of fieldsArray's "update()" (immutable).
     * there is some inconsistency in the form state
     * to fix that we manually set default values form values on reset.
     * TODO: Replace "setValue()" with "update()" when we fix the issue https://github.com/airbytehq/airbyte/issues/31820
     */
    reset({ ...formState.defaultValues });
    if (schemaHasBeenRefreshed) {
      clearFormChange("schemaHasBeenRefreshed");
    }
    onCancel();
  };

  return (
    <Box mt="md">
      <FlexContainer justifyContent="flex-end" alignItems="center" direction="row" gap="lg">
        <ResponseMessage dirty={isDirty} successMessage={successMessage} errorMessage={errorMessage} />
        <FlexContainer gap="md">
          <Button
            type="button"
            variant="secondary"
            disabled={isControlDisabled}
            onClick={onCancelButtonClick}
            data-testid="cancel-edit-button"
          >
            <FormattedMessage id="form.cancel" />
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
      </FlexContainer>
    </Box>
  );
};
