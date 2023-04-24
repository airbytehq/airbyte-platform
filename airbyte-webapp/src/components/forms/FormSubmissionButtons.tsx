import { useFormContext } from "react-hook-form";
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
  const { formState, reset } = useFormContext();

  return (
    <FlexContainer justifyContent="flex-end">
      <Button
        type="button"
        variant="secondary"
        disabled={formState.isSubmitting || !formState.isDirty}
        onClick={() => reset()}
      >
        <FormattedMessage id={cancelKey} />
      </Button>
      <Button type="submit" disabled={!formState.isDirty} isLoading={formState.isSubmitting}>
        <FormattedMessage id={submitKey} />
      </Button>
    </FlexContainer>
  );
};
