import { useFormContext, useFormState } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex/FlexContainer";

interface FormSubmissionButtonsProps {
  submitKey?: string;
  cancelKey?: string;
}

export const FormSubmissionButtons: React.FC<FormSubmissionButtonsProps> = ({
  submitKey = "form.submit",
  cancelKey = "form.cancel",
}) => {
  // get isDirty and isSubmitting from useFormState to avoid re-rendering of whole form if they change
  // reset is a stable function so it's fine to get it from useFormContext
  const { reset } = useFormContext();
  const { isDirty, isSubmitting, isValid } = useFormState();

  return (
    <FlexContainer justifyContent="flex-end">
      <Button type="button" variant="secondary" disabled={isSubmitting || !isDirty} onClick={() => reset()}>
        <FormattedMessage id={cancelKey} />
      </Button>
      <Button type="submit" disabled={!isDirty || !isValid} isLoading={isSubmitting}>
        <FormattedMessage id={submitKey} />
      </Button>
    </FlexContainer>
  );
};
