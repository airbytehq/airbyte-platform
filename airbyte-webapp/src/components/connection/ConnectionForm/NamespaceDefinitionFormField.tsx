import { useCallback } from "react";
import { Controller, useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { ControlLabels } from "components/LabeledControl";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";
import { TextInputContainer } from "components/ui/TextInputContainer";

import { NamespaceDefinitionType } from "core/api/types/AirbyteClient";
import { useModalService } from "hooks/services/Modal";

import { FormConnectionFormValues } from "./formConfig";
import { FormFieldLayout } from "./FormFieldLayout";
import { namespaceDefinitionOptions } from "./types";
import { DestinationNamespaceModal, DestinationNamespaceFormValues } from "../DestinationNamespaceModal";

export const NamespaceDefinitionFormField = () => {
  const { setValue, control } = useFormContext<FormConnectionFormValues>();
  const { openModal, closeModal } = useModalService();

  const namespaceDefinition = useWatch({ name: "namespaceDefinition", control });
  const namespaceFormat = useWatch({ name: "namespaceFormat", control });

  const destinationNamespaceChange = useCallback(
    (value: DestinationNamespaceFormValues) => {
      setValue("namespaceDefinition", value.namespaceDefinition, { shouldDirty: true });

      if (value.namespaceDefinition === NamespaceDefinitionType.customformat) {
        setValue("namespaceFormat", value.namespaceFormat);
        return;
      }
      setValue("namespaceFormat", undefined);
    },
    [setValue]
  );

  const openDestinationNamespaceModal = useCallback(
    () =>
      openModal({
        size: "lg",
        title: <FormattedMessage id="connectionForm.modal.destinationNamespace.title" />,
        content: () => (
          <DestinationNamespaceModal
            initialValues={{
              namespaceDefinition,
              namespaceFormat,
            }}
            onCloseModal={closeModal}
            onSubmit={destinationNamespaceChange}
          />
        ),
      }),
    [closeModal, destinationNamespaceChange, namespaceDefinition, namespaceFormat, openModal]
  );

  return (
    <Controller
      name="namespaceDefinition"
      control={control}
      render={({ field }) => (
        <FormFieldLayout>
          <ControlLabels
            label={<FormattedMessage id="connectionForm.namespaceDefinition.title" />}
            infoTooltipContent={<FormattedMessage id="connectionForm.namespaceDefinition.subtitle" />}
          />
          <FlexContainer alignItems="center" justifyContent="space-between" gap="sm">
            <TextInputContainer disabled>
              <Text>
                {field.value === NamespaceDefinitionType.customformat ? (
                  `${namespaceFormat}`
                ) : (
                  <FormattedMessage id={`connectionForm.${namespaceDefinitionOptions[field.value]}`} />
                )}
              </Text>
            </TextInputContainer>
            <Button
              type="button"
              variant="secondary"
              onClick={openDestinationNamespaceModal}
              data-testid="destination-namespace-edit-button"
            >
              <FormattedMessage id="form.edit" />
            </Button>
          </FlexContainer>
        </FormFieldLayout>
      )}
    />
  );
};
