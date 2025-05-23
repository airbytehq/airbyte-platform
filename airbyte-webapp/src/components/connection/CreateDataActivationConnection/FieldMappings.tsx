import { useMemo } from "react";
import { Controller, useFieldArray, useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Input } from "components/ui/Input";
import { ListBox } from "components/ui/ListBox";
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
        <Field streamIndex={streamIndex} fieldIndex={index} removeField={() => remove(index)} />
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
  removeField: () => void;
}) => {
  const { register, control } = useFormContext<StreamMappingsFormValues>();
  const { formatMessage } = useIntl();
  const source = useGetSourceFromSearchParams();
  const { data: sourceSchema } = useDiscoverSchemaQuery(source.sourceId);
  const sourceStreamDescriptor = useWatch({
    name: `streams.${streamIndex}.sourceStreamDescriptor`,
  });

  const availableFieldOptions = useMemo(() => {
    const streamFields =
      sourceSchema?.catalog?.streams.find(
        ({ stream }) =>
          stream?.name === sourceStreamDescriptor.name && stream?.namespace === sourceStreamDescriptor.namespace
      )?.stream?.jsonSchema?.properties ?? [];
    return Object.keys(streamFields)
      .map((key) => ({ label: key, value: key }))
      .sort((a, b) => {
        return a.label?.localeCompare(b.label ?? "", undefined, { numeric: true }) ?? 0;
      });
  }, [sourceSchema?.catalog?.streams, sourceStreamDescriptor.name, sourceStreamDescriptor.namespace]);

  return (
    <>
      <FlexContainer className={styles.fieldMappings__leftGutter} alignItems="center">
        <Text size="lg">
          <FormattedMessage id="connection.create.map" />
        </Text>
      </FlexContainer>
      <div className={styles.fieldMappings__source}>
        <Controller
          name={`streams.${streamIndex}.fields.${fieldIndex}.sourceFieldName`}
          control={control}
          render={({ field }) => (
            <ListBox options={availableFieldOptions} onSelect={field.onChange} selectedValue={field.value} />
          )}
        />
      </div>
      <div className={styles.fieldMappings__arrow}>
        <Icon type="arrowRight" size="lg" color="action" />
      </div>
      <FlexContainer className={styles.fieldMappings__destination} alignItems="center">
        <Input
          {...register(`streams.${streamIndex}.fields.${fieldIndex}.destinationFieldName`)}
          placeholder={formatMessage({ id: "connection.destinationFieldName" })}
        />
        <Button
          variant="clear"
          icon="trash"
          type="button"
          onClick={removeField}
          aria-label={formatMessage({ id: "connection.create.removeField" })}
        />
      </FlexContainer>
    </>
  );
};
