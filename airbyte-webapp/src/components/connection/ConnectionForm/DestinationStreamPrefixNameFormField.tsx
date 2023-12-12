import { useCallback } from "react";
import { Controller, useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";
import { TextInputContainer } from "components/ui/TextInputContainer";

import { useModalService } from "hooks/services/Modal";

import { FormConnectionFormValues } from "./formConfig";
import { FormFieldLayout } from "./FormFieldLayout";
import { ControlLabels } from "../../LabeledControl";
import {
  DestinationStreamNamesFormValues,
  DestinationStreamNamesModal,
  StreamNameDefinitionValueType,
} from "../DestinationStreamNamesModal";

export const DestinationStreamPrefixNameFormField = () => {
  const { formatMessage } = useIntl();
  const { setValue, control } = useFormContext<FormConnectionFormValues>();
  const { openModal, closeModal } = useModalService();
  const prefix = useWatch({ name: "prefix", control });

  const destinationStreamNamesChange = useCallback(
    (value: DestinationStreamNamesFormValues) => {
      setValue(
        "prefix",
        value.streamNameDefinition === StreamNameDefinitionValueType.Prefix ? value.prefix : undefined,
        { shouldDirty: true }
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
              onClick={openDestinationStreamNamesModal}
              data-testid="destination-stream-prefix-edit-button"
            >
              <FormattedMessage id="form.edit" />
            </Button>
          </FlexContainer>
        </FormFieldLayout>
      )}
    />
  );
};
