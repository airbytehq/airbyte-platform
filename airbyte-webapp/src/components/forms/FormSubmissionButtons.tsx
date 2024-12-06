import { useFormContext, useFormState } from "react-hook-form";
import { FormattedMessage } from "react-intl";
import { useEffectOnce } from "react-use";

import { FormConnectionFormValues } from "components/connection/ConnectionForm/formConfig";
import { Button } from "components/ui/Button";
import { FlexItem } from "components/ui/Flex";
import { FlexContainer } from "components/ui/Flex/FlexContainer";
import { Text } from "components/ui/Text";

import styles from "./FormSubmissionButtons.module.scss";

interface FormSubmissionButtonsProps {
  submitKey?: string;
  cancelKey?: string;
  allowNonDirtyCancel?: boolean;
  allowNonDirtySubmit?: boolean;
  onCancelClickCallback?: () => void;
  justify?: "flex-start" | "flex-end";
  reversed?: boolean;
  noCancel?: boolean;
}

export const FormSubmissionButtons: React.FC<FormSubmissionButtonsProps> = ({
  submitKey = "form.submit",
  cancelKey = "form.cancel",
  allowNonDirtyCancel = false,
  allowNonDirtySubmit = false,
  onCancelClickCallback,
  justify = "flex-end",
  noCancel,
  reversed = false,
}) => {
  // get isSubmitting from useFormState to avoid re-rendering of whole form if they change
  // reset is a stable function so it's fine to get it from useFormContext
  const { reset } = useFormContext();
  const { isValid, isDirty, isSubmitting, errors } = useFormState<FormConnectionFormValues>();

  // need to trigger validation on mount to make error message exist
  const { trigger } = useFormContext();
  useEffectOnce(() => {
    trigger("syncCatalog.streams");
  });
  const hashOtherMappersDefinedError =
    errors?.syncCatalog?.streams?.message === "connectionForm.streams.existingMappers";

  return (
    <FlexContainer justifyContent={justify} className={reversed ? styles.reversed : undefined}>
      {hashOtherMappersDefinedError && (
        <FlexItem alignSelf="center">
          <Text color="red">
            <FormattedMessage id="connectionForm.streams.existingMappers" />
          </Text>
        </FlexItem>
      )}
      {!noCancel && (
        <Button
          type="button"
          variant="secondary"
          disabled={(!isDirty && !allowNonDirtyCancel) || isSubmitting}
          onClick={() => {
            reset();
            onCancelClickCallback?.();
          }}
        >
          <FormattedMessage id={cancelKey} />
        </Button>
      )}
      <Button type="submit" isLoading={isSubmitting} disabled={!isValid || (!isDirty && !allowNonDirtySubmit)}>
        <FormattedMessage id={submitKey} />
      </Button>
    </FlexContainer>
  );
};
