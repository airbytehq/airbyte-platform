import React from "react";
import { useFormContext } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import * as yup from "yup";

import { FormikConnectionFormValues } from "components/connection/ConnectionForm/formConfig";
import { Form, FormControl } from "components/forms";
import { ModalFormSubmissionButtons } from "components/forms/ModalFormSubmissionButtons";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { ModalBody, ModalFooter } from "components/ui/Modal";
import { Text } from "components/ui/Text";
import { InfoTooltip } from "components/ui/Tooltip";

import { LabeledRadioButtonFormControl } from "../ConnectionForm/LabeledRadioButtonFormControl";

export const enum StreamNameDefinitionValueHookFormType {
  Mirror = "mirror",
  Prefix = "prefix",
}

export interface DestinationStreamNamesHookFormValueType {
  streamNameDefinition: StreamNameDefinitionValueHookFormType;
  prefix?: string;
}

const StreamNamePrefixInput: React.FC = () => {
  const { formatMessage } = useIntl();
  const { watch } = useFormContext<DestinationStreamNamesHookFormValueType>();
  const watchedStreamNameDefinition = watch("streamNameDefinition");

  return (
    <FormControl
      name="prefix"
      fieldType="input"
      type="text"
      placeholder={formatMessage({
        id: "connectionForm.modal.destinationStreamNames.input.placeholder",
      })}
      data-testid="destination-stream-names-prefix-input"
      disabled={watchedStreamNameDefinition !== StreamNameDefinitionValueHookFormType.Prefix}
    />
  );
};

const destinationStreamNamesValidationSchema = yup.object().shape({
  streamNameDefinition: yup
    .mixed<StreamNameDefinitionValueHookFormType>()
    .oneOf([StreamNameDefinitionValueHookFormType.Mirror, StreamNameDefinitionValueHookFormType.Prefix])
    .required("form.empty.error"),
  prefix: yup.string().when("streamNameDefinition", {
    is: StreamNameDefinitionValueHookFormType.Prefix,
    then: yup.string().trim().required("form.empty.error"),
  }),
});

interface DestinationStreamNamesHookFormModalProps {
  initialValues: Pick<FormikConnectionFormValues, "prefix">;
  onCloseModal: () => void;
  onSubmit: (value: DestinationStreamNamesHookFormValueType) => void;
}

export const DestinationStreamNamesHookFormModal: React.FC<DestinationStreamNamesHookFormModalProps> = ({
  initialValues,
  onCloseModal,
  onSubmit,
}) => {
  const { formatMessage } = useIntl();

  const onSubmitCallback = async (values: DestinationStreamNamesHookFormValueType) => {
    onCloseModal();
    onSubmit(values);
  };

  return (
    <Form
      defaultValues={{
        streamNameDefinition: initialValues.prefix
          ? StreamNameDefinitionValueHookFormType.Prefix
          : StreamNameDefinitionValueHookFormType.Mirror,
        prefix: initialValues.prefix ?? "",
      }}
      schema={destinationStreamNamesValidationSchema}
      onSubmit={onSubmitCallback}
    >
      <ModalBody padded>
        <FlexContainer direction="column">
          <Box mb="xl">
            <Text color="grey300">
              <FormattedMessage id="connectionForm.modal.destinationStreamNames.description" />
            </Text>
          </Box>
          <FlexContainer direction="column" gap="xl">
            <LabeledRadioButtonFormControl
              name="streamNameDefinition"
              controlId="destinationStreamNames.mirror"
              label={formatMessage({ id: "connectionForm.modal.destinationStreamNames.radioButton.mirror" })}
              value={StreamNameDefinitionValueHookFormType.Mirror}
              data-testid="destination-stream-names-mirror-radio"
            />
            <LabeledRadioButtonFormControl
              name="streamNameDefinition"
              controlId="destinationStreamNames.prefix"
              label={
                <>
                  <FormattedMessage id="connectionForm.modal.destinationStreamNames.radioButton.prefix" />
                  <InfoTooltip placement="top-start">
                    <FormattedMessage id="connectionForm.modal.destinationStreamNames.prefix.message" />
                  </InfoTooltip>
                </>
              }
              value={StreamNameDefinitionValueHookFormType.Prefix}
              data-testid="destination-stream-names-prefix-radio"
            />

            <Box ml="xl">
              <StreamNamePrefixInput />
            </Box>
          </FlexContainer>
        </FlexContainer>
      </ModalBody>
      <ModalFooter>
        <ModalFormSubmissionButtons
          submitKey="form.apply"
          onCancelClickCallback={onCloseModal}
          additionalCancelButtonProps={{ "data-testid": "destination-stream-names-cancel-button" }}
          additionalSubmitButtonProps={{ "data-testid": "destination-stream-names-apply-button" }}
        />
      </ModalFooter>
    </Form>
  );
};
