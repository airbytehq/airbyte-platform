import { useMemo } from "react";
import { Controller, useWatch } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { FormControlErrorMessage } from "components/forms/FormControl";
import { Button } from "components/ui/Button";
import { ComboBox } from "components/ui/ComboBox";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { DataActivationConnectionFormValues } from "area/dataActivation/types";
import { getDestinationOperationFields } from "area/dataActivation/utils/getDestinationOperationFields";
import { getRequiredFields } from "area/dataActivation/utils/getRequiredFields";
import { useSelectedDestinationOperation } from "area/dataActivation/utils/useSelectedDestinationOperation";
import { AirbyteCatalog, DestinationCatalog } from "core/api/types/AirbyteClient";
import { getJsonSchemaType } from "core/domain/catalog";

import { AdditionalMappers } from "./AdditionalMappers";
import styles from "./FieldMapping.module.scss";

interface FieldMappingProps {
  destinationCatalog: DestinationCatalog;
  fieldIndex: number;
  removeField?: () => void;
  sourceCatalog: AirbyteCatalog;
  streamIndex: number;
}

export const FieldMapping: React.FC<FieldMappingProps> = ({
  streamIndex,
  fieldIndex,
  removeField,
  sourceCatalog,
  destinationCatalog,
}) => {
  const { formatMessage } = useIntl();
  const sourceStreamDescriptor = useWatch<
    DataActivationConnectionFormValues,
    `streams.${number}.sourceStreamDescriptor`
  >({
    name: `streams.${streamIndex}.sourceStreamDescriptor`,
  });
  const matchingKeys = useWatch<DataActivationConnectionFormValues, `streams.${number}.matchingKeys`>({
    name: `streams.${streamIndex}.matchingKeys`,
  });
  const fields = useWatch<DataActivationConnectionFormValues, `streams.${number}.fields`>({
    name: `streams.${streamIndex}.fields`,
  });

  const selectedFieldNames = useMemo(() => {
    const sourceFields = new Set<string>();
    const destinationFields = new Set<string>();
    fields
      ?.filter((_, index) => index !== fieldIndex)
      .forEach((field) => {
        if (field.sourceFieldName) {
          sourceFields.add(field.sourceFieldName);
        }
        if (field.destinationFieldName) {
          destinationFields.add(field.destinationFieldName);
        }
      });
    return { sourceFields, destinationFields };
  }, [fieldIndex, fields]);

  const availableSourceFieldOptions = useMemo(() => {
    const topLevelFields =
      sourceCatalog.streams.find(({ stream }) => {
        return stream?.name === sourceStreamDescriptor.name && stream?.namespace === sourceStreamDescriptor.namespace;
      })?.stream?.jsonSchema?.properties ?? [];
    return Object.entries(topLevelFields)
      .map(([key, value]) => ({
        label: key,
        description: getJsonSchemaType(value.type),
        value: key,
        disabled: selectedFieldNames.sourceFields.has(key),
      }))
      .sort((a, b) => {
        return a.label?.localeCompare(b.label ?? "", undefined, { numeric: true }) ?? 0;
      });
  }, [
    selectedFieldNames.sourceFields,
    sourceCatalog.streams,
    sourceStreamDescriptor.name,
    sourceStreamDescriptor.namespace,
  ]);

  const selectedDestinationOperation = useSelectedDestinationOperation(destinationCatalog, streamIndex);

  const additionalPropertiesSupported = !!selectedDestinationOperation?.schema.additionalProperties;

  const availableDestinationFieldOptions = useMemo(() => {
    return getDestinationOperationFields(selectedDestinationOperation)
      .map(([key, value]) => ({
        label: key,
        value: key,
        description: getJsonSchemaType(value.type),
        disabled: selectedFieldNames.destinationFields.has(key),
      }))
      .sort((a, b) => {
        return a.label?.localeCompare(b.label ?? "", undefined, { numeric: true }) ?? 0;
      });
  }, [selectedDestinationOperation, selectedFieldNames.destinationFields]);

  const isPartOfMatchingKey = matchingKeys?.includes(fields[fieldIndex].destinationFieldName) ?? false;

  const isRequiredBySchema = useMemo(() => {
    if (!selectedDestinationOperation) {
      return false;
    }
    const requiredFields = getRequiredFields(selectedDestinationOperation);
    return requiredFields.includes(fields[fieldIndex].destinationFieldName);
  }, [fields, fieldIndex, selectedDestinationOperation]);

  const isRequired = isPartOfMatchingKey || isRequiredBySchema;

  return (
    <div className={styles.fieldMapping}>
      <FlexContainer className={styles.fieldMapping__leftGutter} alignItems="center">
        <Text size="lg">
          <FormattedMessage id="connection.create.map" />
        </Text>
      </FlexContainer>
      <div className={styles.fieldMapping__source}>
        <Controller
          name={`streams.${streamIndex}.fields.${fieldIndex}.sourceFieldName`}
          render={({ field, fieldState }) => (
            <FlexContainer direction="column" gap="xs">
              <ComboBox
                error={!!fieldState.error}
                value={field.value}
                onChange={field.onChange}
                options={availableSourceFieldOptions}
                placeholder={formatMessage({ id: "connection.sourceFieldNamePlaceholder" })}
              />
              {!!fieldState.error && <FormControlErrorMessage name={field.name} />}
            </FlexContainer>
          )}
        />
      </div>
      <div className={styles.fieldMapping__arrow}>
        <Icon type="arrowRight" size="lg" color="action" />
      </div>
      <FlexContainer className={styles.fieldMapping__destination} alignItems="flex-start">
        <FlexItem grow>
          <FlexContainer direction="column">
            <Tooltip
              disabled={!!selectedDestinationOperation && !isRequired}
              control={
                <FlexItem grow>
                  <Controller
                    name={`streams.${streamIndex}.fields.${fieldIndex}.destinationFieldName`}
                    render={({ field, fieldState }) => (
                      <FlexContainer direction="column" gap="xs">
                        <ComboBox
                          disabled={isRequired}
                          error={!!fieldState.error}
                          value={field.value}
                          onChange={field.onChange}
                          options={availableDestinationFieldOptions}
                          placeholder={formatMessage({ id: "connection.destinationFieldNamePlaceholder" })}
                          allowCustomValue={additionalPropertiesSupported}
                        />
                        {!!fieldState.error && <FormControlErrorMessage name={field.name} />}
                      </FlexContainer>
                    )}
                  />
                </FlexItem>
              }
            >
              {!selectedDestinationOperation && (
                <FormattedMessage id="connection.dataActivation.selectDestinationSyncModeFirst" />
              )}
              {isPartOfMatchingKey && <FormattedMessage id="connection.dataActivation.requiredMatchingKey" />}
              {isRequiredBySchema && <FormattedMessage id="connection.dataActivation.requiredField" />}
            </Tooltip>
          </FlexContainer>
        </FlexItem>
        {removeField && (
          <div className={styles.fieldMapping__removeField}>
            <Tooltip
              disabled={!isRequired}
              control={
                <Button
                  disabled={isRequired}
                  variant="clear"
                  icon="trash"
                  type="button"
                  onClick={removeField}
                  aria-label={formatMessage({ id: "connection.create.removeField" })}
                />
              }
            >
              {isPartOfMatchingKey && <FormattedMessage id="connection.dataActivation.requiredMatchingKey" />}
              {isRequiredBySchema && <FormattedMessage id="connection.dataActivation.requiredField" />}
            </Tooltip>
          </div>
        )}
      </FlexContainer>
      <div className={styles.fieldMapping__additionalMappers}>
        <AdditionalMappers streamIndex={streamIndex} fieldIndex={fieldIndex} />
      </div>
    </div>
  );
};
