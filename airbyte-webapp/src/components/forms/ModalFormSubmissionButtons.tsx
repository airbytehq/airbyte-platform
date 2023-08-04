import React from "react";
import { useFormContext, useFormState } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { Button, ButtonProps } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex/FlexContainer";

interface FormSubmissionButtonsProps {
  submitKey?: string;
  cancelKey?: string;
  onCancelClickCallback?: () => void;
  enableCancelWhileClean?: boolean;
  additionalCancelButtonProps?: ButtonProps;
  additionalSubmitButtonProps?: ButtonProps;
}

export const ModalFormSubmissionButtons: React.FC<FormSubmissionButtonsProps> = ({
  submitKey = "form.submit",
  cancelKey = "form.cancel",
  onCancelClickCallback,
  additionalCancelButtonProps,
  additionalSubmitButtonProps,
}) => {
  // get isSubmitting from useFormState to avoid re-rendering of whole form if they change
  // reset is a stable function so it's fine to get it from useFormContext
  const { reset } = useFormContext();
  const { isSubmitting } = useFormState();

  return (
    <FlexContainer justifyContent="flex-end">
      <Button
        type="button"
        variant="secondary"
        disabled={isSubmitting}
        onClick={() => {
          reset();
          onCancelClickCallback?.();
        }}
        {...additionalCancelButtonProps}
      >
        <FormattedMessage id={cancelKey} />
      </Button>
      <Button type="submit" isLoading={isSubmitting} {...additionalSubmitButtonProps}>
        <FormattedMessage id={submitKey} />
      </Button>
    </FlexContainer>
  );
};
