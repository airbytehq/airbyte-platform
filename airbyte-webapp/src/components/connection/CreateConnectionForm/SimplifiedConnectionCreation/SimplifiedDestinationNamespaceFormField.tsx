import { ComponentProps, useEffect } from "react";
import { Controller, useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { FormConnectionFormValues } from "components/connection/ConnectionForm/formConfig";
import { FormFieldLayout } from "components/connection/ConnectionForm/FormFieldLayout";
import { RadioButtonTiles } from "components/connection/CreateConnection/RadioButtonTiles";
import { FormControl } from "components/forms";
import { ControlLabels } from "components/LabeledControl";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { useGetSourceFromSearchParams } from "area/connector/utils";
import { NamespaceDefinitionType } from "core/api/types/AirbyteClient";

export const SimplifiedDestinationNamespaceFormField = () => {
  const source = useGetSourceFromSearchParams();
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
      description: "connectionForm.customFormatDescriptionNext",
      extra:
        namespaceDefinition === NamespaceDefinitionType.customformat ? (
          <FormControl
            name="namespaceFormat"
            fieldType="input"
            type="text"
            placeholder={source.sourceName.toLowerCase().replace(/[^a-z0-9]+/g, "_")}
            data-testid="namespace-definition-custom-format-input"
          />
        ) : null,
    },
    {
      value: NamespaceDefinitionType.destination,
      label: "connectionForm.destinationFormatNext",
      description: "connectionForm.destinationFormatDescriptionNext",
    },
    {
      value: NamespaceDefinitionType.source,
      label: "connectionForm.sourceFormatNext",
      description: "connectionForm.sourceFormatDescriptionNext",
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
              <FlexContainer direction="column" gap="sm">
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
