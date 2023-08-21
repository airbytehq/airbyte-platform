import { Field, FieldProps, useFormikContext } from "formik";
import { useCallback } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { ControlLabels } from "components/LabeledControl";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";
import { TextInputContainer } from "components/ui/TextInputContainer";

import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useModalService } from "hooks/services/Modal";

import { FormikConnectionFormValues } from "./formConfig";
import { FormFieldLayout } from "./FormFieldLayout";
import {
  DestinationStreamNamesModal,
  DestinationStreamNamesFormValues,
  StreamNameDefinitionValueType,
} from "../DestinationStreamNamesModal";

export const DestinationStreamPrefixName = () => {
  const { mode } = useConnectionFormService();
  const { formatMessage } = useIntl();
  const { openModal, closeModal } = useModalService();
  const formikProps = useFormikContext<FormikConnectionFormValues>();

  const destinationStreamNamesHookFormChange = useCallback(
    (value: DestinationStreamNamesFormValues) => {
      formikProps.setFieldValue(
        "prefix",
        value.streamNameDefinition === StreamNameDefinitionValueType.Prefix ? value.prefix : ""
      );
    },
    [formikProps]
  );

  const openDestinationStreamNamesModal = useCallback(
    () =>
      openModal({
        size: "sm",
        title: <FormattedMessage id="connectionForm.modal.destinationStreamNames.title" />,
        content: () => (
          <DestinationStreamNamesModal
            initialValues={{
              prefix: formikProps.values.prefix,
            }}
            onCloseModal={closeModal}
            onSubmit={destinationStreamNamesHookFormChange}
          />
        ),
      }),
    [closeModal, destinationStreamNamesHookFormChange, formikProps.values.prefix, openModal]
  );
  return (
    <Field name="prefix">
      {({ field }: FieldProps<string>) => (
        <FormFieldLayout>
          <ControlLabels
            nextLine
            optional
            label={formatMessage({
              id: "form.prefix",
            })}
            infoTooltipContent={formatMessage({
              id: "form.prefix.message",
            })}
          />
          <FlexContainer alignItems="center" justifyContent="space-between" gap="sm">
            <TextInputContainer disabled>
              <Text>
                {!field.value
                  ? formatMessage({ id: "connectionForm.modal.destinationStreamNames.radioButton.mirror" })
                  : field.value}
              </Text>
            </TextInputContainer>
            <Button
              type="button"
              variant="secondary"
              disabled={mode === "readonly"}
              onClick={openDestinationStreamNamesModal}
              data-testid="destination-stream-prefix-edit-button"
            >
              <FormattedMessage id="form.edit" />
            </Button>
          </FlexContainer>
        </FormFieldLayout>
      )}
    </Field>
  );
};
