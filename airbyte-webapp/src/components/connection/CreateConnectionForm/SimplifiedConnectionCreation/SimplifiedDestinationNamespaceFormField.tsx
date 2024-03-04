import { ComponentProps, useEffect } from "react";
import { Controller, useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { FormConnectionFormValues } from "components/connection/ConnectionForm/formConfig";
import { FormFieldLayout } from "components/connection/ConnectionForm/FormFieldLayout";
import { RadioButtonTiles } from "components/connection/CreateConnection/RadioButtonTiles";
import { FormControl } from "components/forms";
import { ControlLabels } from "components/LabeledControl";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { NamespaceDefinitionType } from "core/api/types/AirbyteClient";

export const SimplifiedDestinationNamespaceFormField = () => {
  const { formatMessage } = useIntl();
  const { trigger, setValue, control, watch } = useFormContext<FormConnectionFormValues>();
  const namespaceDefinition = useWatch({ name: "namespaceDefinition", control });

  const watchedNamespaceDefinition = watch("namespaceDefinition");
  useEffect(() => {
    trigger("namespaceFormat", { shouldFocus: true });
  }, [trigger, watchedNamespaceDefinition]);

  const destinationNamespaceOptions: ComponentProps<typeof RadioButtonTiles<NamespaceDefinitionType>>["options"] = [
    {
      value: NamespaceDefinitionType.customformat,
      label: "connectionForm.customFormat",
      description: "connectionForm.customFormatDescription",
      extra:
        namespaceDefinition === NamespaceDefinitionType.customformat ? (
          <FormControl
            name="namespaceFormat"
            fieldType="input"
            type="text"
            placeholder={formatMessage({
              id: "connectionForm.modal.destinationNamespace.input.placeholder",
            })}
            data-testid="namespace-definition-custom-format-input"
          />
        ) : null,
    },
    {
      value: NamespaceDefinitionType.destination,
      label: "connectionForm.destinationFormat",
      description: "connectionForm.destinationFormatDescription",
    },
    {
      value: NamespaceDefinitionType.source,
      label: "connectionForm.sourceFormat",
      description: "connectionForm.sourceFormatDescription",
    },
  ];

  return (
    <Controller
      name="namespaceDefinition"
      control={control}
      render={({ field }) => (
        <FormFieldLayout alignItems="flex-start" nextSizing>
          <ControlLabels
            label={
              <FlexContainer direction="column">
                <Text bold>
                  <FormattedMessage id="connectionForm.namespaceDefinition.title" />
                </Text>
                <Text size="sm" color="grey">
                  <FormattedMessage id="connectionForm.namespaceDefinition.subtitleNext" />
                </Text>
              </FlexContainer>
            }
          />
          <RadioButtonTiles
            direction="column"
            name="destinationNamespace"
            options={destinationNamespaceOptions}
            selectedValue={field.value}
            onSelectRadioButton={(value) => setValue("namespaceDefinition", value, { shouldDirty: true })}
          />
        </FormFieldLayout>
      )}
    />
  );
};
