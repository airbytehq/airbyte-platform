import { useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage } from "react-intl";
import { useAsync, useEffectOnce } from "react-use";
import * as yup from "yup";

import { Button } from "components/ui/Button";
import { ModalBody, ModalFooter } from "components/ui/Modal";

import { FormGroupItem, FormObjectArrayItem } from "core/form/types";

import { FormSection } from "./FormSection";
import { useConnectorForm } from "../../connectorFormContext";
import { setDefaultValues } from "../../useBuildForm";

interface VariableInputFormProps {
  formField: FormObjectArrayItem;
  path: string;
  item?: Record<string, unknown>;
  disabled?: boolean;
  onDone: (value: unknown) => void;
  onCancel: () => void;
}

export const VariableInputFieldForm: React.FC<VariableInputFormProps> = ({
  formField,
  path,
  item,
  disabled,
  onDone,
  onCancel,
}) => {
  const { setValue } = useFormContext();
  // use exact: false to also watch for changes in nested fields to catch changes to the valid state of this part of the form
  const value = useWatch({ name: path, exact: false });
  const { validationSchema } = useConnectorForm();

  // Copy the validation from the original field to ensure that the form has all the required values field out correctly.
  const { value: isValid } = useAsync(
    async (): Promise<boolean> => yup.reach(validationSchema, path).isValid(value),
    [value, path, validationSchema]
  );

  useEffectOnce(() => {
    let initialValue = item;
    // if no item is passed in, we are creating a new one, so we need to set the default values
    if (!initialValue) {
      initialValue = {};
      setDefaultValues(formField.properties as FormGroupItem, initialValue, { respectExistingValues: true });
    }

    setValue(path, initialValue);
  });

  return (
    <>
      <ModalBody maxHeight={300}>
        <FormSection blocks={formField.properties} path={path} disabled={disabled} skipAppend />
      </ModalBody>
      <ModalFooter>
        <Button
          data-testid="cancel-button"
          variant="secondary"
          onClick={() => {
            onCancel();
          }}
        >
          <FormattedMessage id="form.cancel" />
        </Button>
        <Button
          data-testid="done-button"
          disabled={disabled || !isValid}
          onClick={() => {
            onDone(value);
          }}
        >
          <FormattedMessage id="form.done" />
        </Button>
      </ModalFooter>
    </>
  );
};
