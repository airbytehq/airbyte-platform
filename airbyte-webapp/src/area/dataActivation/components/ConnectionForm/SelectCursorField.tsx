import { Controller, useFormContext } from "react-hook-form";
import { useIntl } from "react-intl";

import { DataActivationConnectionFormValues } from "area/dataActivation/types";
import { AirbyteCatalog } from "core/api/types/AirbyteClient";

import { LabeledListbox } from "./LabeledListbox";
import { useSelectedSourceStream } from "./StreamMappings";

interface SelectCursorFieldProps {
  sourceCatalog: AirbyteCatalog;
  streamIndex: number;
}

export const SelectCursorField: React.FC<SelectCursorFieldProps> = ({ sourceCatalog, streamIndex }) => {
  const { control } = useFormContext<DataActivationConnectionFormValues>();
  const { formatMessage } = useIntl();

  const selectedSourceStream = useSelectedSourceStream(sourceCatalog, streamIndex);
  return (
    <Controller
      control={control}
      name={`streams.${streamIndex}.cursorField`}
      render={({ field, fieldState }) => (
        <LabeledListbox
          fieldName={field.name}
          iconType="cursor"
          value={field.value}
          onChange={field.onChange}
          options={Object.keys(selectedSourceStream?.stream?.jsonSchema?.properties ?? {})
            .map((key) => ({
              label: key,
              value: key,
            }))
            .sort((a, b) => a.label.localeCompare(b.label))}
          label={formatMessage({ id: "connection.dataActivation.cursor" })}
          hasError={!!fieldState.error}
        />
      )}
    />
  );
};
