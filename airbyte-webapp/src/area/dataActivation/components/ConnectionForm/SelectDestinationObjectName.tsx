import { useMemo } from "react";
import { Controller, useFormContext } from "react-hook-form";
import { useIntl } from "react-intl";

import { ConnectorIcon } from "components/ConnectorIcon";
import { FormControlErrorMessage } from "components/forms/FormControl";
import { ComboBox } from "components/ui/ComboBox";
import { FlexContainer } from "components/ui/Flex";

import { DataActivationConnectionFormValues } from "area/dataActivation/types";
import { DestinationCatalog, DestinationRead, DestinationSyncMode } from "core/api/types/AirbyteClient";

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
  const { control, setValue } = useFormContext<DataActivationConnectionFormValues>();

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

  const syncModesByObjectName = useMemo(() => {
    const map = new Map<string, Set<DestinationSyncMode>>();
    destinationCatalog.operations.forEach((operation) => {
      if (!map.has(operation.objectName)) {
        map.set(operation.objectName, new Set<DestinationSyncMode>());
      }
      map.get(operation.objectName)?.add(operation.syncMode);
    });
    return map;
  }, [destinationCatalog]);

  return (
    <div className={styles.selectDestinationObjectName}>
      <Controller
        name={`streams.${streamIndex}.destinationObjectName`}
        control={control}
        render={({ field, fieldState }) => (
          <FlexContainer direction="column" gap="xs">
            <ComboBox
              error={!!fieldState.error}
              options={destinationObjectNameOptions}
              value={field.value}
              icon={<ConnectorIcon icon={destination.icon} className={styles.selectDestinationObjectName__icon} />}
              onChange={(value) => {
                if (value === field.value) {
                  return;
                }

                field.onChange(value);
                setValue(`streams.${streamIndex}.matchingKeys`, null);
                if (syncModesByObjectName.get(value)?.size === 1) {
                  // If there is only one sync mode available for the selected object name, set it automatically
                  setValue(
                    `streams.${streamIndex}.destinationSyncMode`,
                    syncModesByObjectName.get(value)?.values().next().value ?? null
                  );
                }
              }}
              placeholder={formatMessage({ id: "connection.create.selectDestinationObject" })}
            />
            {fieldState.error && <FormControlErrorMessage name={field.name} />}
          </FlexContainer>
        )}
      />
    </div>
  );
};
