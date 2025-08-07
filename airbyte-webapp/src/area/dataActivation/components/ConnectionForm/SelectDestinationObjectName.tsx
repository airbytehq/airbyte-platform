import React, { useCallback, useMemo } from "react";
import { Controller, useFormContext } from "react-hook-form";
import { useIntl } from "react-intl";

import { ConnectorIcon } from "components/ConnectorIcon";
import { FormControlErrorMessage } from "components/forms/FormControl";
import { FlexContainer } from "components/ui/Flex";

import { DataActivationConnectionFormValues } from "area/dataActivation/types";
import { EMPTY_FIELD } from "area/dataActivation/utils";
import { useSetDefaultValuesForDestinationOperation } from "area/dataActivation/utils/useSetDefaultValuesForDestinationOperation";
import { DestinationCatalog, DestinationRead } from "core/api/types/AirbyteClient";

import { DACombobox } from "./DACombobox";
import styles from "./SelectDestinationObjectName.module.scss";

interface SelectDestinationObjectNameProps {
  destination: DestinationRead;
  destinationCatalog: DestinationCatalog;
  streamIndex: number;
}
export const SelectDestinationObjectName: React.FC<SelectDestinationObjectNameProps> = ({
  destination,
  destinationCatalog,
  streamIndex,
}) => {
  const { formatMessage } = useIntl();
  const { control, getValues, setValue } = useFormContext<DataActivationConnectionFormValues>();

  const setDefaultValuesForDestinationOperation = useSetDefaultValuesForDestinationOperation();

  const destinationObjectNameOptions = useMemo(() => {
    const destinationObjectNames = new Set(destinationCatalog.operations.map((operation) => operation.objectName));
    return Array.from(destinationObjectNames)
      .map((objectName) => ({
        label: objectName,
        value: objectName,
      }))
      .sort((a, b) => {
        return a.label?.localeCompare(b.label ?? "", undefined, { numeric: true }) ?? 0;
      });
  }, [destinationCatalog]);

  const resetFormValues = useCallback(() => {
    const destinationObjectName = getValues(`streams.${streamIndex}.destinationObjectName`);

    const availableOperations = destinationCatalog.operations.filter(
      (operation) => operation.objectName === destinationObjectName
    );

    // Auto-select the sync mode if there is only one operation available
    if (availableOperations.length === 1) {
      setValue(`streams.${streamIndex}.destinationSyncMode`, availableOperations[0].syncMode);
      setDefaultValuesForDestinationOperation(availableOperations[0], streamIndex);
    } else {
      // Multiple operations are available, so reset everything and make the user choose one
      setValue(`streams.${streamIndex}.destinationSyncMode`, null);
      setValue(`streams.${streamIndex}.matchingKeys`, null);
      setValue(`streams.${streamIndex}.fields`, [EMPTY_FIELD]);
    }
  }, [destinationCatalog.operations, getValues, setDefaultValuesForDestinationOperation, setValue, streamIndex]);

  return (
    <div className={styles.selectDestinationObjectName}>
      <Controller
        name={`streams.${streamIndex}.destinationObjectName`}
        control={control}
        render={({ field, fieldState }) => (
          <FlexContainer direction="column" gap="xs">
            <DACombobox
              error={!!fieldState.error}
              placeholder={formatMessage({ id: "connection.create.selectDestinationObject" })}
              icon={<ConnectorIcon icon={destination.icon} className={styles.selectDestinationObjectName__icon} />}
              options={destinationObjectNameOptions}
              selectedValue={field.value}
              onChange={(value) => {
                if (value === field.value) {
                  return;
                }
                if (value === null) {
                  // We don't want to set the field to null, because that would violate the zod schema
                  field.onChange("");
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
