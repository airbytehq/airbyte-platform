import React from "react";
import { useFormContext } from "react-hook-form";
import { useIntl } from "react-intl";
import * as yup from "yup";

import { FormikConnectionFormValues } from "components/connection/ConnectionForm/formConfig";
import { Form, FormControl } from "components/forms";
import { ModalFormSubmissionButtons } from "components/forms/ModalFormSubmissionButtons";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { ModalBody, ModalFooter } from "components/ui/Modal";

import { NamespaceDefinitionType } from "core/request/AirbyteClient";

import { DestinationNamespaceDescription } from "./DestinationNamespaceDescription";
import styles from "./DestinationNamespaceHookFormModal.module.scss";
import { LabeledRadioButtonFormControl } from "../ConnectionForm/LabeledRadioButtonFormControl";

export interface DestinationNamespaceHookFormValueType {
  namespaceDefinition: NamespaceDefinitionType;
  namespaceFormat?: string;
}

const NameSpaceCustomFormatInput: React.FC = () => {
  const { formatMessage } = useIntl();
  const { watch } = useFormContext<DestinationNamespaceHookFormValueType>();
  const watchedNamespaceDefinition = watch("namespaceDefinition");

  return (
    <FormControl
      name="namespaceFormat"
      fieldType="input"
      type="text"
      placeholder={formatMessage({
        id: "connectionForm.modal.destinationNamespace.input.placeholder",
      })}
      data-testid="namespace-definition-custom-format-input"
      disabled={watchedNamespaceDefinition !== NamespaceDefinitionType.customformat}
    />
  );
};

const destinationNamespaceValidationSchema = yup.object().shape({
  namespaceDefinition: yup
    .mixed<NamespaceDefinitionType>()
    .oneOf(Object.values(NamespaceDefinitionType))
    .required("form.empty.error"),
  namespaceFormat: yup.string().when("namespaceDefinition", {
    is: NamespaceDefinitionType.customformat,
    then: yup.string().trim().required("form.empty.error"),
  }),
});

interface DestinationNamespaceHookFormModalProps {
  initialValues: Pick<FormikConnectionFormValues, "namespaceDefinition" | "namespaceFormat">;
  onCloseModal: () => void;
  onSubmit: (values: DestinationNamespaceHookFormValueType) => void;
}

export const DestinationNamespaceHookFormModal: React.FC<DestinationNamespaceHookFormModalProps> = ({
  initialValues,
  onCloseModal,
  onSubmit,
}) => {
  const { formatMessage } = useIntl();

  const onSubmitCallback = async (values: DestinationNamespaceHookFormValueType) => {
    onCloseModal();
    onSubmit(values);
  };
  return (
    <Form
      defaultValues={{
        namespaceDefinition: initialValues?.namespaceDefinition ?? NamespaceDefinitionType.destination,
        namespaceFormat: initialValues.namespaceFormat,
      }}
      schema={destinationNamespaceValidationSchema}
      onSubmit={onSubmitCallback}
    >
      <>
        <ModalBody className={styles.content} padded={false}>
          <FlexContainer direction="column" gap="xl" className={styles.actions}>
            <LabeledRadioButtonFormControl
              name="namespaceDefinition"
              controlId="destinationNamespace.destination"
              label={formatMessage({ id: "connectionForm.modal.destinationNamespace.option.destination" })}
              value={NamespaceDefinitionType.destination}
              data-testid="namespace-definition-destination-radio"
            />
            <LabeledRadioButtonFormControl
              name="namespaceDefinition"
              controlId="destinationNamespace.source"
              label={formatMessage({ id: "connectionForm.modal.destinationNamespace.option.source" })}
              value={NamespaceDefinitionType.source}
              data-testid="namespace-definition-source-radio"
            />
            <LabeledRadioButtonFormControl
              name="namespaceDefinition"
              controlId="destinationNamespace.customFormat"
              label={formatMessage({ id: "connectionForm.modal.destinationNamespace.option.customFormat" })}
              value={NamespaceDefinitionType.customformat}
              data-testid="namespace-definition-custom-format-radio"
            />
            <Box ml="xl">
              <NameSpaceCustomFormatInput />
            </Box>
          </FlexContainer>
          <FlexContainer direction="column" className={styles.description}>
            <DestinationNamespaceDescription />
          </FlexContainer>
        </ModalBody>
        <ModalFooter>
          <ModalFormSubmissionButtons
            submitKey="form.apply"
            onCancelClickCallback={onCloseModal}
            additionalCancelButtonProps={{ "data-testid": "namespace-definition-cancel-button" }}
            additionalSubmitButtonProps={{ "data-testid": "namespace-definition-apply-button" }}
          />
        </ModalFooter>
      </>
    </Form>
  );
};
