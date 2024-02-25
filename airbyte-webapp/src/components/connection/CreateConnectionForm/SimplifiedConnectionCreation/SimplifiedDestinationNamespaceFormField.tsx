import { ComponentProps } from "react";
import { Controller, useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { FormConnectionFormValues } from "components/connection/ConnectionForm/formConfig";
import { FormFieldLayout } from "components/connection/ConnectionForm/FormFieldLayout";
import { RadioButtonTiles } from "components/connection/CreateConnection/RadioButtonTiles";
import { ControlLabels } from "components/LabeledControl";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { NamespaceDefinitionType } from "core/api/types/AirbyteClient";

import { SimplifiedDestinationCustomFormatFormField } from "./SimplifiedDestinationCustomFormatFormField";

export const SimplifiedDestinationNamespaceFormField = () => {
  const { setValue, control } = useFormContext<FormConnectionFormValues>();
  const namespaceDefinition = useWatch({ name: "namespaceDefinition", control });

  const destinationNamespaceOptions: ComponentProps<typeof RadioButtonTiles<NamespaceDefinitionType>>["options"] = [
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
    {
      value: NamespaceDefinitionType.customformat,
      label: "connectionForm.customFormat",
      description: "connectionForm.customFormatDescription",
      extra:
        namespaceDefinition === NamespaceDefinitionType.customformat ? (
          <SimplifiedDestinationCustomFormatFormField />
        ) : null,
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
                  <FormattedMessage id="connectionForm.namespaceDefinition.subtitle" />
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
