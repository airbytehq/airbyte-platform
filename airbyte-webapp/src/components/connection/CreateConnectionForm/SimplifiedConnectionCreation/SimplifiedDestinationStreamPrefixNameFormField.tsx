import uniqueId from "lodash/uniqueId";
import { useCallback, useState } from "react";
import { Controller, useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { FormConnectionFormValues } from "components/connection/ConnectionForm/formConfig";
import { FormFieldLayout } from "components/connection/ConnectionForm/FormFieldLayout";
import {
  DestinationStreamNamesFormValues,
  DestinationStreamNamesModal,
  StreamNameDefinitionValueType,
} from "components/connection/DestinationStreamNamesModal";
import { ControlLabels } from "components/LabeledControl";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";
import { TextInputContainer } from "components/ui/TextInputContainer";

import { useModalService } from "hooks/services/Modal";

import { InputContainer } from "./InputContainer";

export const SimplifiedDestinationStreamPrefixNameFormField = () => {
  const { formatMessage } = useIntl();
  const { setValue, control } = useFormContext<FormConnectionFormValues>();
  const [controlId] = useState(`input-control-${uniqueId()}`);
  const { openModal, closeModal } = useModalService();
  const prefix = useWatch({ name: "prefix", control });

  const destinationStreamNamesChange = useCallback(
    (value: DestinationStreamNamesFormValues) => {
      setValue(
        "prefix",
        value.streamNameDefinition === StreamNameDefinitionValueType.Prefix ? value.prefix ?? "" : "",
        {
          shouldDirty: true,
        }
      );
    },
    [setValue]
  );

  const openDestinationStreamNamesModal = useCallback(
    () =>
      openModal({
        size: "sm",
        title: <FormattedMessage id="connectionForm.modal.destinationStreamNames.title" />,
        content: () => (
          <DestinationStreamNamesModal
            initialValues={{
              prefix,
            }}
            onCloseModal={closeModal}
            onSubmit={destinationStreamNamesChange}
          />
        ),
      }),
    [closeModal, destinationStreamNamesChange, openModal, prefix]
  );

  return (
    <Controller
      name="prefix"
      control={control}
      render={({ field }) => (
        <FormFieldLayout alignItems="flex-start" nextSizing>
          <ControlLabels
            htmlFor={controlId}
            label={
              <FlexContainer direction="column">
                <Text bold>
                  <FormattedMessage id="form.prefixNext" />
                </Text>
                <Text size="sm" color="grey" italicized>
                  <FormattedMessage id="form.prefix.subtitle" />
                </Text>
              </FlexContainer>
            }
            infoTooltipContent={formatMessage({
              id: "form.prefix.message",
            })}
          />
          <InputContainer>
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
                onClick={openDestinationStreamNamesModal}
                data-testid="destination-stream-prefix-edit-button"
              >
                <FormattedMessage id="form.edit" />
              </Button>
            </FlexContainer>
          </InputContainer>
        </FormFieldLayout>
      )}
    />
  );
};
