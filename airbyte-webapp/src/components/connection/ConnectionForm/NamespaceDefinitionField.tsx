import { Field, FieldProps, useFormikContext } from "formik";
import { useCallback } from "react";
import { FormattedMessage } from "react-intl";

import { ControlLabels } from "components/LabeledControl";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";
import { TextInputContainer } from "components/ui/TextInputContainer";

import { NamespaceDefinitionType } from "core/request/AirbyteClient";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useExperiment } from "hooks/services/Experiment";
import { useModalService } from "hooks/services/Modal";

import { FormikConnectionFormValues } from "./formConfig";
import { FormFieldLayout } from "./FormFieldLayout";
import { namespaceDefinitionOptions } from "./types";
import {
  DestinationNamespaceHookFormModal,
  DestinationNamespaceHookFormValueType,
} from "../DestinationNamespaceModal/DestinationNamespaceHookFormModal";
import {
  DestinationNamespaceFormValueType,
  DestinationNamespaceModal,
} from "../DestinationNamespaceModal/DestinationNamespaceModal";

export const NamespaceDefinitionField = () => {
  const { mode } = useConnectionFormService();
  const { openModal, closeModal } = useModalService();
  const doUseReactHookForm = useExperiment("form.reactHookForm", false);

  const formikProps = useFormikContext<FormikConnectionFormValues>();

  const destinationNamespaceHookFormChange = useCallback(
    (value: DestinationNamespaceHookFormValueType) => {
      formikProps.setFieldValue("namespaceDefinition", value.namespaceDefinition);

      if (value.namespaceDefinition === NamespaceDefinitionType.customformat) {
        formikProps.setFieldValue("namespaceFormat", value.namespaceFormat);
      }
    },
    [formikProps]
  );

  // TODO: remove after DestinationNamespaceModal migration
  const destinationNamespaceChange = useCallback(
    (value: DestinationNamespaceFormValueType) => {
      formikProps.setFieldValue("namespaceDefinition", value.namespaceDefinition);

      if (value.namespaceDefinition === NamespaceDefinitionType.customformat) {
        formikProps.setFieldValue("namespaceFormat", value.namespaceFormat);
      }
    },
    [formikProps]
  );

  const openDestinationNamespaceModal = useCallback(
    () =>
      openModal({
        size: "lg",
        title: <FormattedMessage id="connectionForm.modal.destinationNamespace.title" />,
        content: () =>
          doUseReactHookForm ? (
            <DestinationNamespaceHookFormModal
              initialValues={{
                namespaceDefinition: formikProps.values.namespaceDefinition,
                namespaceFormat: formikProps.values.namespaceFormat,
              }}
              onCloseModal={closeModal}
              onSubmit={destinationNamespaceHookFormChange}
            />
          ) : (
            <DestinationNamespaceModal
              initialValues={{
                namespaceDefinition: formikProps.values.namespaceDefinition,
                namespaceFormat: formikProps.values.namespaceFormat,
              }}
              onCloseModal={closeModal}
              onSubmit={destinationNamespaceChange}
            />
          ),
      }),
    [
      closeModal,
      destinationNamespaceChange,
      destinationNamespaceHookFormChange,
      doUseReactHookForm,
      formikProps.values.namespaceDefinition,
      formikProps.values.namespaceFormat,
      openModal,
    ]
  );
  return (
    <Field name="namespaceDefinition">
      {({ field }: FieldProps<NamespaceDefinitionType>) => (
        <FormFieldLayout>
          <ControlLabels
            label={<FormattedMessage id="connectionForm.namespaceDefinition.title" />}
            infoTooltipContent={<FormattedMessage id="connectionForm.namespaceDefinition.subtitle" />}
          />
          <FlexContainer alignItems="center" justifyContent="space-between" gap="sm">
            <TextInputContainer disabled>
              <Text>
                <FormattedMessage id={`connectionForm.${namespaceDefinitionOptions[field.value]}`} />
              </Text>
            </TextInputContainer>
            <Button
              type="button"
              variant="secondary"
              disabled={mode === "readonly"}
              onClick={openDestinationNamespaceModal}
              data-testid="destination-namespace-edit-button"
            >
              <FormattedMessage id="form.edit" />
            </Button>
          </FlexContainer>
        </FormFieldLayout>
      )}
    </Field>
  );
};
