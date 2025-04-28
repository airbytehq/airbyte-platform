import get from "lodash/get";
import { ComponentProps, useEffect } from "react";
import { Controller, useFormContext, useFormState, useWatch } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { FormConnectionFormValues } from "components/connection/ConnectionForm/formConfig";
import { FormFieldLayout } from "components/connection/ConnectionForm/FormFieldLayout";
import { RadioButtonTiles } from "components/connection/CreateConnection/RadioButtonTiles";
import { ControlLabels } from "components/LabeledControl";
import { Badge } from "components/ui/Badge";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Input } from "components/ui/Input";
import { ExternalLink } from "components/ui/Link";
import { ListBox } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import { DestinationRead, NamespaceDefinitionType, SourceRead } from "core/api/types/AirbyteClient";
import { links } from "core/utils/links";
import { naturalComparator } from "core/utils/objects";

import { DestinationNamespaceConfiguration, SourceNamespaceConfiguration } from "./ConnectorNamespaceConfiguration";
import { InputContainer } from "./InputContainer";
import styles from "./SimplifiedDestinationNamespaceFormField.module.scss";

// eslint-disable-next-line no-template-curly-in-string
const SOURCE_NAMESPACE_REPLACEMENT_STRING = "${SOURCE_NAMESPACE}";

export const SimplifiedDestinationNamespaceFormField: React.FC<{
  isCreating: boolean;
  source: SourceRead;
  destination: DestinationRead;
  disabled?: boolean;
}> = ({ isCreating, source, destination, disabled }) => {
  const { trigger, setValue, control, watch } = useFormContext<FormConnectionFormValues>();
  const { defaultValues } = useFormState<FormConnectionFormValues>();
  const namespaceDefinition = useWatch({ name: "namespaceDefinition", control });
  const streams = useWatch({ name: "syncCatalog.streams", control });
  const namespaceFormat = useWatch({ name: "namespaceFormat", control });
  const { formatMessage } = useIntl();

  const watchedNamespaceDefinition = watch("namespaceDefinition");
  useEffect(() => {
    trigger("namespaceFormat", { shouldFocus: true });
  }, [trigger, setValue, defaultValues?.namespaceFormat, watchedNamespaceDefinition]);

  const sourceNamespaceAbilities = SourceNamespaceConfiguration[source.sourceDefinitionId] ?? {
    supportsNamespaces: true,
  };
  const destinationNamespaceAbilities = DestinationNamespaceConfiguration[destination.destinationDefinitionId] ?? {
    supportsNamespaces: true,
  };

  if (!destinationNamespaceAbilities.supportsNamespaces) {
    return null;
  }

  const defaultNamespacePath =
    destinationNamespaceAbilities.defaultNamespacePath &&
    get(destination.connectionConfiguration, destinationNamespaceAbilities.defaultNamespacePath);
  const destinationDefinedNamespace =
    typeof defaultNamespacePath === "string" ? defaultNamespacePath : "no_value_provided";

  const destinationDefinedDescriptionValues = {
    destinationDefinedNamespace,
    badge: (children: React.ReactNode[]) => (
      <Badge variant="grey" className={styles.originalCasing}>
        {children}
      </Badge>
    ),
  };

  const enabledStreamNamespaces = Array.from(
    streams.reduce((acc, stream) => {
      if (stream.config?.selected && stream.stream?.namespace) {
        acc.add(stream.stream.namespace);
      }
      return acc;
    }, new Set<string>())
  ).sort(naturalComparator);

  const sourceDefinedDescriptionValues = {
    sourceDefinedNamespaces: enabledStreamNamespaces,
    badges: (children: React.ReactNode[]) => {
      if (children.length === 0) {
        return null;
      }

      return (
        <Box mt="sm">
          {children.map((child, idx) => (
            <Badge key={idx} variant="grey" className={styles.sourceNamespace} data-testid="source-namespace-preview">
              {child}
            </Badge>
          ))}
        </Box>
      );
    },
  };

  const customFormatField =
    namespaceDefinition === NamespaceDefinitionType.customformat ? (
      <>
        <Controller
          name="namespaceFormat"
          control={control}
          render={({ field, fieldState }) => (
            <Box mt="sm">
              <InputContainer>
                <Input
                  name="namespaceFormat"
                  placeholder={source.name.toLowerCase().replace(/[^a-z0-9]+/g, "_")}
                  inline={false}
                  value={field.value}
                  onChange={field.onChange}
                  disabled={disabled}
                  data-testid="namespace-definition-custom-format-input"
                />
              </InputContainer>
              {fieldState.error && (
                <Box mt="sm">
                  <Text color="red">
                    <FormattedMessage id={fieldState.error.message} />
                  </Text>
                </Box>
              )}
            </Box>
          )}
        />
        {namespaceFormat?.includes(SOURCE_NAMESPACE_REPLACEMENT_STRING) &&
          (enabledStreamNamespaces.length > 0 ? enabledStreamNamespaces : [""]).map((namespace) => {
            return (
              <Badge
                key={namespace}
                variant="grey"
                className={styles.sourceNamespace}
                data-testid="custom-namespace-preview"
              >
                {namespaceFormat.replace(SOURCE_NAMESPACE_REPLACEMENT_STRING, namespace)}
              </Badge>
            );
          })}
      </>
    ) : null;

  const destinationNamespaceOptions: ComponentProps<typeof RadioButtonTiles<NamespaceDefinitionType>>["options"] = [
    {
      value: NamespaceDefinitionType.customformat,
      label: formatMessage({ id: "connectionForm.customFormat" }),
      description: formatMessage({ id: "connectionForm.customFormatDescriptionNext" }),
      extra: customFormatField,
      "data-testid": "custom",
    },
    {
      value: NamespaceDefinitionType.destination,
      label: formatMessage({ id: "connectionForm.destinationFormatNext" }),
      description: formatMessage(
        { id: "connectionForm.destinationFormatDescriptionNext" },
        destinationDefinedDescriptionValues
      ),
      "data-testid": "destination",
    },
    ...(sourceNamespaceAbilities.supportsNamespaces
      ? [
          {
            value: NamespaceDefinitionType.source,
            label: formatMessage({ id: "connectionForm.sourceFormatNext" }),
            description: formatMessage(
              { id: "connectionForm.sourceFormatDescriptionNext" },
              sourceDefinedDescriptionValues
            ),
            "data-testid": "source",
          },
        ]
      : []),
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
                data-testid="namespace-definition"
              />
              {field.value === NamespaceDefinitionType.destination && (
                <Box mt="sm">
                  <Text size="sm">
                    <FormattedMessage
                      id="connectionForm.destinationFormatDescriptionNext"
                      values={destinationDefinedDescriptionValues}
                    />
                  </Text>
                </Box>
              )}
              {field.value === NamespaceDefinitionType.source && (
                <Box mt="sm">
                  <Text size="sm">
                    <FormattedMessage
                      id="connectionForm.sourceFormatDescriptionNext"
                      values={sourceDefinedDescriptionValues}
                    />
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
