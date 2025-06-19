import { useMemo } from "react";
import { useFieldArray, useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { FormControl } from "components/forms/FormControl";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { DataActivationConnectionFormValues } from "area/dataActivation/types";
import { AirbyteCatalog, DestinationCatalog } from "core/api/types/AirbyteClient";

import styles from "./FieldMappings.module.scss";

interface FieldMappingsProps {
  destinationCatalog: DestinationCatalog;
  sourceCatalog: AirbyteCatalog;
  streamIndex: number;
}

export const FieldMappings: React.FC<FieldMappingsProps> = ({ destinationCatalog, sourceCatalog, streamIndex }) => {
  const { control } = useFormContext<DataActivationConnectionFormValues>();
  const { fields, append, remove } = useFieldArray({
    control,
    name: `streams.${streamIndex}.fields`,
  });

  return (
    <>
      {fields.map((_field, index) => (
        <Field
          destinationCatalog={destinationCatalog}
          sourceCatalog={sourceCatalog}
          key={_field.id}
          streamIndex={streamIndex}
          fieldIndex={index}
          removeField={fields.length > 1 ? () => remove(index) : undefined}
        />
      ))}
      <Box py="sm" className={styles.fieldMappings__addField}>
        <Button
          icon="plus"
          variant="secondary"
          type="button"
          onClick={() => append({ sourceFieldName: "", destinationFieldName: "" })}
        >
          <FormattedMessage id="connection.create.addField" />
        </Button>
      </Box>
    </>
  );
};

interface FieldProps {
  destinationCatalog: DestinationCatalog;
  fieldIndex: number;
  removeField?: () => void;
  sourceCatalog: AirbyteCatalog;
  streamIndex: number;
}

const Field: React.FC<FieldProps> = ({ streamIndex, fieldIndex, removeField, sourceCatalog, destinationCatalog }) => {
  const { formatMessage } = useIntl();
  const sourceStreamDescriptor = useWatch<
    DataActivationConnectionFormValues,
    `streams.${number}.sourceStreamDescriptor`
  >({
    name: `streams.${streamIndex}.sourceStreamDescriptor`,
  });
  const destinationObjectName = useWatch<DataActivationConnectionFormValues, `streams.${number}.destinationObjectName`>(
    {
      name: `streams.${streamIndex}.destinationObjectName`,
    }
  );
  const destinationSyncMode = useWatch<DataActivationConnectionFormValues, `streams.${number}.destinationSyncMode`>({
    name: `streams.${streamIndex}.destinationSyncMode`,
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
    return Object.keys(topLevelFields)
      .map((key) => ({
        label: key,
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

  const selectedDestinationOperation = useMemo(() => {
    // An operation is identifiable by the combination of its objectName and syncMode.
    return destinationCatalog.operations.find(
      (operation) => operation.objectName === destinationObjectName && operation.syncMode === destinationSyncMode
    );
  }, [destinationCatalog, destinationObjectName, destinationSyncMode]);

  const availableDestinationFieldOptions = useMemo(() => {
    const topLevelFields = selectedDestinationOperation?.schema?.properties ?? [];
    return Object.keys(topLevelFields)
      .map((key) => ({
        label: key,
        value: key,
        disabled: selectedFieldNames.destinationFields.has(key),
      }))
      .sort((a, b) => {
        return a.label?.localeCompare(b.label ?? "", undefined, { numeric: true }) ?? 0;
      });
  }, [selectedDestinationOperation?.schema?.properties, selectedFieldNames.destinationFields]);

  return (
    <>
      <FlexContainer className={styles.fieldMappings__leftGutter} alignItems="center">
        <Text size="lg">
          <FormattedMessage id="connection.create.map" />
        </Text>
      </FlexContainer>
      <div className={styles.fieldMappings__source}>
        <FormControl<DataActivationConnectionFormValues>
          options={availableSourceFieldOptions}
          name={`streams.${streamIndex}.fields.${fieldIndex}.sourceFieldName`}
          fieldType="dropdown"
          reserveSpaceForError={false}
        />
      </div>
      <div className={styles.fieldMappings__arrow}>
        <Icon type="arrowRight" size="lg" color="action" />
      </div>
      <FlexContainer className={styles.fieldMappings__destination} alignItems="flex-start">
        <FlexItem grow>
          <FlexContainer direction="column">
            <Tooltip
              disabled={!!destinationSyncMode}
              control={
                <FlexItem grow>
                  <FormControl<DataActivationConnectionFormValues>
                    options={availableDestinationFieldOptions}
                    name={`streams.${streamIndex}.fields.${fieldIndex}.destinationFieldName`}
                    disabled={!destinationSyncMode}
                    fieldType="dropdown"
                    reserveSpaceForError={false}
                  />
                </FlexItem>
              }
            >
              {!destinationSyncMode && (
                <FormattedMessage id="connection.dataActivation.selectDestinationSyncModeFirst" />
              )}
            </Tooltip>
          </FlexContainer>
        </FlexItem>
        {removeField && (
          <div className={styles.fieldMappings__removeField}>
            <Button
              variant="clear"
              icon="trash"
              type="button"
              onClick={removeField}
              aria-label={formatMessage({ id: "connection.create.removeField" })}
            />
          </div>
        )}
      </FlexContainer>
    </>
  );
};
