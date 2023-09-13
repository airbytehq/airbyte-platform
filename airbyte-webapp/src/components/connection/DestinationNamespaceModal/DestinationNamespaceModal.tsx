import React from "react";
import { useFormContext } from "react-hook-form";
import { useIntl } from "react-intl";
import * as yup from "yup";

import { Form, FormControl } from "components/forms";
import { ModalFormSubmissionButtons } from "components/forms/ModalFormSubmissionButtons";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { ModalBody, ModalFooter } from "components/ui/Modal";

import { NamespaceDefinitionType } from "core/request/AirbyteClient";

import { DestinationNamespaceDescription } from "./DestinationNamespaceDescription";
import styles from "./DestinationNamespaceModal.module.scss";
import { FormikConnectionFormValues } from "../ConnectionForm/formConfig";
import {
  HookFormConnectionFormValues,
  namespaceDefinitionSchema,
  namespaceFormatSchema,
} from "../ConnectionForm/hookFormConfig";
import { LabeledRadioButtonFormControl } from "../ConnectionForm/LabeledRadioButtonFormControl";

export interface DestinationNamespaceFormValues {
  namespaceDefinition: NamespaceDefinitionType;
  namespaceFormat?: string;
}

const NameSpaceCustomFormatInput: React.FC = () => {
  const { formatMessage } = useIntl();
  const { watch } = useFormContext<DestinationNamespaceFormValues>();
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
  namespaceDefinition: namespaceDefinitionSchema.required("form.empty.error"),
  namespaceFormat: namespaceFormatSchema,
});

interface DestinationNamespaceModalProps {
  /**
   * temporary extend this interface since we use modal in Formik and react-hook-form forms
   *TODO: remove FormikConnectionFormValues after successful CreateConnectionForm migration
   *https://github.com/airbytehq/airbyte-platform-internal/issues/8639
   */
  initialValues: Pick<
    FormikConnectionFormValues | HookFormConnectionFormValues,
    "namespaceDefinition" | "namespaceFormat"
  >;
  onCloseModal: () => void;
  onSubmit: (values: DestinationNamespaceFormValues) => void;
}

export const DestinationNamespaceModal: React.FC<DestinationNamespaceModalProps> = ({
  initialValues,
  onCloseModal,
  onSubmit,
}) => {
  const { formatMessage } = useIntl();

  const onSubmitCallback = async (values: DestinationNamespaceFormValues) => {
    onCloseModal();
    onSubmit(values);
  };
  return (
    <Form
      defaultValues={{
        namespaceDefinition: initialValues.namespaceDefinition,
        // eslint-disable-next-line no-template-curly-in-string
        namespaceFormat: initialValues.namespaceFormat ?? "${SOURCE_NAMESPACE}",
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
