import isEqual from "lodash/isEqual";
import { useCallback, useMemo } from "react";
import { Controller, useFormContext } from "react-hook-form";
import { useIntl } from "react-intl";

import { ConnectorIcon } from "components/ConnectorIcon";
import { FormControlErrorMessage } from "components/forms/FormControl";
import { FlexContainer } from "components/ui/Flex";

import { DataActivationConnectionFormValues } from "area/dataActivation/types";
import { EMPTY_FIELD } from "area/dataActivation/utils";
import { AirbyteCatalog, SourceRead } from "core/api/types/AirbyteClient";

import { DACombobox } from "./DACombobox";
import styles from "./SelectSourceStream.module.scss";

interface SelectSourceStreamProps {
  index: number;
  source: SourceRead;
  sourceCatalog: AirbyteCatalog;
}

export const SelectSourceStream: React.FC<SelectSourceStreamProps> = ({ sourceCatalog, source, index }) => {
  const { control, getValues, setValue } = useFormContext<DataActivationConnectionFormValues>();
  const { formatMessage } = useIntl();
  const sourceStreams = useMemo(() => sourceCatalog.streams ?? [], [sourceCatalog]);

  const showNamespace = useMemo(
    () => new Set(sourceStreams.map((stream) => stream.stream?.namespace)).size > 1,
    [sourceStreams]
  );

  const sourceStreamOptions = useMemo(
    () =>
      sourceStreams
        .map((stream) => ({
          label: showNamespace ? `${stream.stream?.namespace}.${stream.stream?.name}` : stream.stream?.name ?? "",
          value: { name: stream.stream?.name ?? "", namespace: stream.stream?.namespace },
        }))
        .sort((a, b) => {
          return a.label?.localeCompare(b.label ?? "", undefined, { numeric: true }) ?? 0;
        }),
    [sourceStreams, showNamespace]
  );

  const resetFormValues = useCallback(() => {
    const fields = getValues(`streams.${index}.fields`);
    const sourceStreamDescriptor = getValues(`streams.${index}.sourceStreamDescriptor`);
    const newFields = fields.length === 0 ? [EMPTY_FIELD] : fields;

    const sourceStream = sourceCatalog.streams.find(
      (stream) =>
        stream.stream?.name === sourceStreamDescriptor.name &&
        stream.stream?.namespace === sourceStreamDescriptor.namespace
    );

    if (sourceStream) {
      const availableSyncModes = sourceStream.stream?.supportedSyncModes;

      if (availableSyncModes?.length === 1) {
        // Automatically select the only available sync mode
        setValue(`streams.${index}.sourceSyncMode`, availableSyncModes[0]);
        setValue(`streams.${index}.cursorField`, null);
      } else {
        setValue(`streams.${index}.sourceSyncMode`, null);
        setValue(`streams.${index}.cursorField`, null);
      }
      const availableSourceFields = getSourceStreamFieldNames(sourceCatalog, sourceStreamDescriptor);

      setValue(
        `streams.${index}.fields`,
        // The source stream likely changed, so we need to check if mappings have valid source field names, otherwise
        // reset them to empty strings, forcing the user to re-select a new one
        newFields.map((field) => ({
          ...field,
          sourceFieldName: availableSourceFields.includes(field.sourceFieldName) ? field.sourceFieldName : "",
        }))
      );
    } else {
      // No source stream is selected, so we need to reset the sync mode and clear source field names
      setValue(`streams.${index}.sourceSyncMode`, null);
      setValue(
        `streams.${index}.fields`,
        newFields.map((field) => ({
          ...field,
          sourceFieldName: "",
        }))
      );
    }
  }, [getValues, index, sourceCatalog, setValue]);

  return (
    <div className={styles.selectSourceStream}>
      <Controller
        name={`streams.${index}.sourceStreamDescriptor`}
        control={control}
        render={({ field, fieldState }) => (
          <FlexContainer direction="column" gap="xs">
            <DACombobox
              error={!!fieldState.error}
              selectedValue={field.value}
              placeholder={formatMessage({ id: "connection.create.selectSourceStream" })}
              icon={<ConnectorIcon icon={source.icon} className={styles.selectSourceStream__icon} />}
              options={sourceStreamOptions}
              onChange={(value) => {
                if (isEqual(value, field.value)) {
                  return;
                }
                if (value === null) {
                  // We don't want to set the field to null, because that would violate the zod schema
                  field.onChange({
                    name: "",
                  });
                } else {
                  field.onChange(value);
                }
                resetFormValues();
              }}
            />
            {fieldState.error && <FormControlErrorMessage name={field.name} />}
          </FlexContainer>
        )}
      />
    </div>
  );
};

const getSourceStreamFieldNames = (
  sourceCatalog: AirbyteCatalog,
  sourceStreamDescriptor: { name: string; namespace?: string }
) => {
  const topLevelFields =
    sourceCatalog.streams.find(({ stream }) => {
      return stream?.name === sourceStreamDescriptor.name && stream?.namespace === sourceStreamDescriptor.namespace;
    })?.stream?.jsonSchema?.properties ?? [];
  return Object.keys(topLevelFields);
};
