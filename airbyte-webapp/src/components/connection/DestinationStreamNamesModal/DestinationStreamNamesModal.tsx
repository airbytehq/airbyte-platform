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

import { HookFormConnectionFormValues } from "../ConnectionForm/hookFormConfig";
import { LabeledRadioButtonFormControl } from "../ConnectionForm/LabeledRadioButtonFormControl";

export const enum StreamNameDefinitionValueType {
  Mirror = "mirror",
  Prefix = "prefix",
}

export interface DestinationStreamNamesFormValues {
  streamNameDefinition: StreamNameDefinitionValueType;
  prefix?: string;
}

const StreamNamePrefixInput: React.FC = () => {
  const { formatMessage } = useIntl();
  const { watch } = useFormContext<DestinationStreamNamesFormValues>();
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
      disabled={watchedStreamNameDefinition !== StreamNameDefinitionValueType.Prefix}
    />
  );
};

const destinationStreamNamesValidationSchema = yup.object().shape({
  streamNameDefinition: yup
    .mixed<StreamNameDefinitionValueType>()
    .oneOf([StreamNameDefinitionValueType.Mirror, StreamNameDefinitionValueType.Prefix])
    .required("form.empty.error"),
  prefix: yup.string().when("streamNameDefinition", {
    is: StreamNameDefinitionValueType.Prefix,
    then: yup.string().trim().required("form.empty.error"),
  }),
});

interface DestinationStreamNamesModalProps {
  /**
   * temporary extend this interface since we use modal in Formik and react-hook-form forms
   *TODO: remove FormikConnectionFormValues after successful CreateConnectionForm migration
   *https://github.com/airbytehq/airbyte-platform-internal/issues/8639
   */
  initialValues: Pick<FormikConnectionFormValues | HookFormConnectionFormValues, "prefix">;
  onCloseModal: () => void;
  onSubmit: (value: DestinationStreamNamesFormValues) => void;
}

export const DestinationStreamNamesModal: React.FC<DestinationStreamNamesModalProps> = ({
  initialValues,
  onCloseModal,
  onSubmit,
}) => {
  const { formatMessage } = useIntl();

  const onSubmitCallback = async (values: DestinationStreamNamesFormValues) => {
    onCloseModal();
    onSubmit(values);
  };

  return (
    <Form
      defaultValues={{
        streamNameDefinition: initialValues.prefix
          ? StreamNameDefinitionValueType.Prefix
          : StreamNameDefinitionValueType.Mirror,
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
              value={StreamNameDefinitionValueType.Mirror}
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
              value={StreamNameDefinitionValueType.Prefix}
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
