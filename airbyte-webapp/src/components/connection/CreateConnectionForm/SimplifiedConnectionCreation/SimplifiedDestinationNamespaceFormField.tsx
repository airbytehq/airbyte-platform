import { ComponentProps, useEffect } from "react";
import { Controller, useFormContext, useFormState, useWatch } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { FormConnectionFormValues } from "components/connection/ConnectionForm/formConfig";
import { FormFieldLayout } from "components/connection/ConnectionForm/FormFieldLayout";
import { RadioButtonTiles } from "components/connection/CreateConnection/RadioButtonTiles";
import { FormControl } from "components/forms";
import { ControlLabels } from "components/LabeledControl";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { ExternalLink } from "components/ui/Link";
import { ListBox } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import { NamespaceDefinitionType } from "core/api/types/AirbyteClient";
import { links } from "core/utils/links";

import { InputContainer } from "./InputContainer";

export const SimplifiedDestinationNamespaceFormField: React.FC<{
  isCreating: boolean;
  sourceName: string;
  disabled?: boolean;
}> = ({ isCreating, sourceName, disabled }) => {
  const { trigger, setValue, control, watch } = useFormContext<FormConnectionFormValues>();
  const { defaultValues } = useFormState<FormConnectionFormValues>();
  const namespaceDefinition = useWatch({ name: "namespaceDefinition", control });
  const { formatMessage } = useIntl();

  const watchedNamespaceDefinition = watch("namespaceDefinition");
  useEffect(() => {
    if (watchedNamespaceDefinition === NamespaceDefinitionType.customformat) {
      setValue("namespaceFormat", defaultValues?.namespaceFormat, { shouldDirty: true });
    }

    trigger("namespaceFormat", { shouldFocus: true });
  }, [trigger, setValue, defaultValues?.namespaceFormat, watchedNamespaceDefinition]);

  const watchedNamespaceFormat = watch("namespaceFormat");
  useEffect(() => {
    trigger("namespaceFormat");
  }, [trigger, watchedNamespaceFormat]);

  const customFormatField =
    namespaceDefinition === NamespaceDefinitionType.customformat ? (
      <FormControl
        name="namespaceFormat"
        fieldType="input"
        type="text"
        placeholder={sourceName.toLowerCase().replace(/[^a-z0-9]+/g, "_")}
        data-testid="namespace-definition-custom-format-input"
        disabled={disabled}
      />
    ) : null;

  const destinationNamespaceOptions: ComponentProps<typeof RadioButtonTiles<NamespaceDefinitionType>>["options"] = [
    {
      value: NamespaceDefinitionType.customformat,
      label: isCreating ? "connectionForm.customFormat" : formatMessage({ id: "connectionForm.customFormat" }),
      description: "connectionForm.customFormatDescriptionNext",
      extra: customFormatField,
    },
    {
      value: NamespaceDefinitionType.destination,
      label: isCreating
        ? "connectionForm.destinationFormatNext"
        : formatMessage({ id: "connectionForm.destinationFormatNext" }),
      description: "connectionForm.destinationFormatDescriptionNext",
    },
    {
      value: NamespaceDefinitionType.source,
      label: isCreating ? "connectionForm.sourceFormatNext" : formatMessage({ id: "connectionForm.sourceFormatNext" }),
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
                  <ExternalLink href={links.namespaceLink}>
                    <Text size="xs" color="blue">
                      <FormattedMessage id="connectionForm.modal.destinationNamespace.learnMore.link" />
                    </Text>
                  </ExternalLink>
                </Text>
              </FlexContainer>
            }
          />
          {isCreating ? (
            <RadioButtonTiles
              direction="column"
              name="destinationNamespace"
              options={destinationNamespaceOptions}
              selectedValue={field.value}
              onSelectRadioButton={(value) => setValue("namespaceDefinition", value, { shouldDirty: true })}
            />
          ) : (
            <InputContainer>
              <ListBox
                isDisabled={disabled}
                options={destinationNamespaceOptions}
                onSelect={(value: NamespaceDefinitionType) =>
                  setValue("namespaceDefinition", value, { shouldDirty: true })
                }
                selectedValue={field.value}
              />
              {field.value === NamespaceDefinitionType.destination && (
                <Box mt="sm">
                  <Text size="sm">
                    <FormattedMessage id="connectionForm.destinationFormatDescriptionNext" />
                  </Text>
                </Box>
              )}
              {field.value === NamespaceDefinitionType.source && (
                <Box mt="sm">
                  <Text size="sm">
                    <FormattedMessage id="connectionForm.sourceFormatDescriptionNext" />
                  </Text>
                </Box>
              )}
              {customFormatField}
            </InputContainer>
          )}
        </FormFieldLayout>
      )}
    />
  );
};
