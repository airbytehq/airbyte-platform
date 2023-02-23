import { Field, FieldProps, useFormikContext } from "formik";
import { useCallback } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useModalService } from "hooks/services/Modal";

import { FormikConnectionFormValues } from "./formConfig";
import { FormFieldWrapper } from "./FormFieldWrapper";
import { ControlLabels } from "../../LabeledControl";
import {
  DestinationStreamNamesFormValueType,
  DestinationStreamNamesModal,
  StreamNameDefinitionValueType,
} from "../DestinationStreamNamesModal/DestinationStreamNamesModal";

export const DestinationStreamPrefixName = () => {
  const { mode } = useConnectionFormService();
  const { formatMessage } = useIntl();
  const { openModal, closeModal } = useModalService();
  const formikProps = useFormikContext<FormikConnectionFormValues>();

  const destinationStreamNamesChange = useCallback(
    (value: DestinationStreamNamesFormValueType) => {
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
            onSubmit={destinationStreamNamesChange}
          />
        ),
      }),
    [closeModal, destinationStreamNamesChange, formikProps.values.prefix, openModal]
  );
  return (
    <Field name="prefix">
      {({ field }: FieldProps<string>) => (
        <FormFieldWrapper>
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
          <FlexContainer alignItems="center" justifyContent="space-between">
            <Text color="grey">
              {field.value === ""
                ? formatMessage({ id: "connectionForm.modal.destinationStreamNames.radioButton.mirror" })
                : field.value}
            </Text>
            <Button
              type="button"
              variant="secondary"
              disabled={mode === "readonly"}
              onClick={openDestinationStreamNamesModal}
            >
              <FormattedMessage id="form.edit" />
            </Button>
          </FlexContainer>
        </FormFieldWrapper>
      )}
    </Field>
  );
};
