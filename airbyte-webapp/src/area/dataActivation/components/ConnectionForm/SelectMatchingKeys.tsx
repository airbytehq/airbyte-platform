import { useMemo } from "react";
import { Controller, UseFieldArrayAppend, useFormContext, useWatch } from "react-hook-form";
import { useIntl } from "react-intl";

import { DataActivationConnectionFormValues } from "area/dataActivation/types";
import { DestinationCatalog } from "core/api/types/AirbyteClient";

import { LabeledListbox } from "./LabeledListbox";

interface SelectMatchingKeyProps {
  appendField: UseFieldArrayAppend<DataActivationConnectionFormValues, "streams" | `streams.${number}.fields`>;
  destinationCatalog: DestinationCatalog;
  streamIndex: number;
}

export const SelectMatchingKey: React.FC<SelectMatchingKeyProps> = ({
  appendField,
  destinationCatalog,
  streamIndex,
}) => {
  const { control } = useFormContext<DataActivationConnectionFormValues>();
  const { formatMessage } = useIntl();
  const destinationOperation = useSelectedDestinationOperation(destinationCatalog, streamIndex);

  const fields = useWatch<DataActivationConnectionFormValues, `streams.${number}.fields`>({
    control,
    name: `streams.${streamIndex}.fields`,
  });
  const destinationFields = fields.map((field) => field.destinationFieldName);

  return (
    <Controller
      control={control}
      name={`streams.${streamIndex}.matchingKeys`}
      render={({ field, fieldState }) => (
        <LabeledListbox
          fieldName={field.name}
          iconType="infoOutline"
          value={field.value}
          onChange={(value) => {
            field.onChange(value);
            const requiredFields = value?.filter((v) => !destinationFields.includes(v));
            requiredFields?.forEach((fieldName) => {
              appendField({
                sourceFieldName: "",
                destinationFieldName: fieldName,
              });
            });
          }}
          // A matchingKey represents a list of properties that are used to identify a record uniquely. It's like a
          // compound primary key, but without nested properties. In data activation, there are multiple possible
          // sets of matching keys
          options={(destinationOperation?.matchingKeys ?? [])
            .map((key) => ({
              label: key.join(", "),
              value: key,
            }))
            .sort((a, b) => a.label.localeCompare(b.label))}
          label={formatMessage({ id: "connection.dataActivation.matchingKey" })}
          hasError={!!fieldState.error}
        />
      )}
    />
  );
};

export function useSelectedDestinationOperation(destinationCatalog: DestinationCatalog, streamIndex: number) {
  const destinationSyncMode = useWatch<DataActivationConnectionFormValues, `streams.${number}.destinationSyncMode`>({
    name: `streams.${streamIndex}.destinationSyncMode`,
  });
  const destinationObjectName = useWatch<DataActivationConnectionFormValues, `streams.${number}.destinationObjectName`>(
    {
      name: `streams.${streamIndex}.destinationObjectName`,
    }
  );
  return useMemo(() => {
    return destinationCatalog.operations.find(
      (operation) => operation.syncMode === destinationSyncMode && operation.objectName === destinationObjectName
    );
  }, [destinationCatalog, destinationObjectName, destinationSyncMode]);
}
