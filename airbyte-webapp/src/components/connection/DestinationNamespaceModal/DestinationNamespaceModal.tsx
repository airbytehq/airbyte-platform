import React, { useEffect } from "react";
import { useFormContext } from "react-hook-form";
import { useIntl } from "react-intl";
import { z } from "zod";

import { Form, FormControl } from "components/forms";
import { ModalFormSubmissionButtons } from "components/forms/ModalFormSubmissionButtons";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { ModalBody, ModalFooter } from "components/ui/Modal";

import { NamespaceDefinitionType } from "core/api/types/AirbyteClient";

import { DestinationNamespaceDescription } from "./DestinationNamespaceDescription";
import styles from "./DestinationNamespaceModal.module.scss";
import { FormConnectionFormValues } from "../ConnectionForm/formConfig";
import { LabeledRadioButtonFormControl } from "../ConnectionForm/LabeledRadioButtonFormControl";
import { namespaceFormatSchema } from "../ConnectionForm/schemas/namespaceDefinitionSchema";

export type DestinationNamespaceFormValues = z.infer<typeof namespaceFormatSchema>;

const NameSpaceCustomFormatInput: React.FC = () => {
  const { formatMessage } = useIntl();
  const { watch, trigger } = useFormContext<DestinationNamespaceFormValues>();
  const watchedNamespaceDefinition = watch("namespaceDefinition");

  useEffect(() => {
    trigger("namespaceFormat");
  }, [trigger, watchedNamespaceDefinition]);

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

interface DestinationNamespaceModalProps {
  initialValues: Pick<FormConnectionFormValues, "namespaceDefinition" | "namespaceFormat">;
  onCancel: () => void;
  onSubmit: (values: DestinationNamespaceFormValues) => Promise<void>;
}

export const DestinationNamespaceModal: React.FC<DestinationNamespaceModalProps> = ({
  initialValues,
  onCancel,
  onSubmit,
}) => {
  const { formatMessage } = useIntl();

  return (
    <Form<DestinationNamespaceFormValues>
      zodSchema={namespaceFormatSchema}
      defaultValues={{
        namespaceDefinition: initialValues.namespaceDefinition,
        // eslint-disable-next-line no-template-curly-in-string
        namespaceFormat: initialValues.namespaceFormat ?? "${SOURCE_NAMESPACE}",
      }}
      onSubmit={onSubmit}
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
              controlId="destinationNamespace.customformat"
              label={formatMessage({ id: "connectionForm.modal.destinationNamespace.option.customformat" })}
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
            onCancelClickCallback={onCancel}
            additionalCancelButtonProps={{ "data-testid": "namespace-definition-cancel-button" }}
            additionalSubmitButtonProps={{ "data-testid": "namespace-definition-apply-button" }}
          />
        </ModalFooter>
      </>
    </Form>
  );
};
