import { useMemo } from "react";
import { useFieldArray, useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { FormControl } from "components/forms/FormControl";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { useGetSourceFromSearchParams } from "area/connector/utils";
import { useDiscoverSchemaQuery } from "core/api";

import styles from "./FieldMappings.module.scss";
import { StreamMappingsFormValues } from "./StreamMappings";

interface FieldMappingsProps {
  streamIndex: number;
}

export const FieldMappings: React.FC<FieldMappingsProps> = ({ streamIndex }) => {
  const { control } = useFormContext<StreamMappingsFormValues>();
  const { fields, append, remove } = useFieldArray({
    control,
    name: `streams.${streamIndex}.fields`,
  });

  return (
    <>
      {fields.map((_field, index) => (
        <Field
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

const Field = ({
  streamIndex,
  fieldIndex,
  removeField,
}: {
  streamIndex: number;
  fieldIndex: number;
  removeField?: () => void;
}) => {
  const { formatMessage } = useIntl();
  const source = useGetSourceFromSearchParams();
  const { data: sourceSchema } = useDiscoverSchemaQuery(source.sourceId);
  const sourceStreamDescriptor = useWatch<StreamMappingsFormValues, `streams.${number}.sourceStreamDescriptor`>({
    name: `streams.${streamIndex}.sourceStreamDescriptor`,
  });
  const fields = useWatch<StreamMappingsFormValues, `streams.${number}.fields`>({
    name: `streams.${streamIndex}.fields`,
  });
  const otherSelectedFields = useMemo(() => {
    return fields?.filter((_, index) => index !== fieldIndex).map((field) => field.sourceFieldName) ?? [];
  }, [fieldIndex, fields]);

  const availableFieldOptions = useMemo(() => {
    const streamFields =
      sourceSchema?.catalog?.streams.find(
        ({ stream }) =>
          stream?.name === sourceStreamDescriptor.name && stream?.namespace === sourceStreamDescriptor.namespace
      )?.stream?.jsonSchema?.properties ?? [];
    return Object.keys(streamFields)
      .map((key) => ({
        label: key,
        value: key,
        disabled: otherSelectedFields?.includes(key),
      }))
      .sort((a, b) => {
        return a.label?.localeCompare(b.label ?? "", undefined, { numeric: true }) ?? 0;
      });
  }, [
    otherSelectedFields,
    sourceSchema?.catalog?.streams,
    sourceStreamDescriptor.name,
    sourceStreamDescriptor.namespace,
  ]);

  return (
    <>
      <FlexContainer className={styles.fieldMappings__leftGutter} alignItems="center">
        <Text size="lg">
          <FormattedMessage id="connection.create.map" />
        </Text>
      </FlexContainer>
      <div className={styles.fieldMappings__source}>
        <FormControl<StreamMappingsFormValues>
          options={availableFieldOptions}
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
            <FormControl<StreamMappingsFormValues>
              name={`streams.${streamIndex}.fields.${fieldIndex}.destinationFieldName`}
              fieldType="input"
              type="text"
              reserveSpaceForError={false}
              placeholder={formatMessage({ id: "connection.destinationFieldName" })}
            />
          </FlexContainer>
        </FlexItem>
        <div className={styles.fieldMappings__removeField}>
          {removeField && (
            <Button
              variant="clear"
              icon="trash"
              type="button"
              onClick={removeField}
              aria-label={formatMessage({ id: "connection.create.removeField" })}
            />
          )}
        </div>
      </FlexContainer>
    </>
  );
};
