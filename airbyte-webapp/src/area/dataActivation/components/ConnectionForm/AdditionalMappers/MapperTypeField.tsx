import { useMemo } from "react";
import { Controller, useFormContext } from "react-hook-form";
import { useIntl } from "react-intl";

import { ListBox } from "components/ui/ListBox";

import { DataActivationConnectionFormValues } from "area/dataActivation/types";
import { StreamMapperType } from "core/api/types/AirbyteClient";
import { FilterCondition } from "pages/connections/ConnectionMappingsPage/RowFilteringMapperForm/RowFilteringMapperForm";

interface MapperTypeFieldProps {
  name: `streams.${number}.fields.${number}.additionalMappers.${number}`;
}

export const MapperTypeField: React.FC<MapperTypeFieldProps> = ({ name }) => {
  const { control } = useFormContext<DataActivationConnectionFormValues>();
  const { formatMessage } = useIntl();

  const options = useMemo<Array<{ label: string; value: StreamMapperType }>>(
    () => [
      { label: formatMessage({ id: "connections.mappings.type.hash" }), value: "hashing" },
      { label: formatMessage({ id: "connections.mappings.type.rowFiltering" }), value: "row-filtering" },
      { label: formatMessage({ id: "connections.mappings.type.encryption" }), value: "encryption" },
    ],
    [formatMessage]
  );

  return (
    <Controller
      control={control}
      name={name}
      render={({ field }) => (
        <ListBox
          options={options}
          selectedValue={field.value.type}
          onSelect={(value) => {
            // Note: we are manually using typeof field.value here, because field.onChange does not have a strict type
            // check: https://github.com/react-hook-form/react-hook-form/pull/10342
            // If react-hook-form ever changes this, we can just pass the values in directly to field.onChange
            if (value === "hashing") {
              const defaultHashingValues: typeof field.value = { type: "hashing", method: "MD5" };
              field.onChange(defaultHashingValues);
            }
            if (value === "row-filtering") {
              const defaultRowFilteringValues: typeof field.value = {
                type: "row-filtering",
                condition: FilterCondition.IN,
                comparisonValue: "",
              };
              field.onChange(defaultRowFilteringValues);
            }
            if (value === "encryption") {
              const defaultEncryptionValues: typeof field.value = { type: "encryption", publicKey: "" };
              field.onChange(defaultEncryptionValues);
            }
          }}
        />
      )}
    />
  );
};
