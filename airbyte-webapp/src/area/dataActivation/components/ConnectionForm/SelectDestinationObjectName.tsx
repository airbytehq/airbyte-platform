import { Listbox } from "@headlessui/react";
import { useMemo } from "react";
import { Controller, useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { ConnectorIcon } from "components/ConnectorIcon";
import { FormControlErrorMessage } from "components/forms/FormControl";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { FloatLayout } from "components/ui/ListBox/FloatLayout";
import { ListboxButton } from "components/ui/ListBox/ListboxButton";
import { ListboxOption } from "components/ui/ListBox/ListboxOption";
import { ListboxOptions } from "components/ui/ListBox/ListboxOptions";
import { Text } from "components/ui/Text";

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
            <Listbox
              onChange={(value) => {
                if (value === field.value) {
                  return;
                }

                field.onChange(value);
                if (syncModesByObjectName.get(value)?.size === 1) {
                  // If there is only one sync mode available for the selected object name, set it automatically
                  setValue(
                    `streams.${streamIndex}.destinationSyncMode`,
                    syncModesByObjectName.get(value)?.values().next().value ?? null
                  );
                }
              }}
              value={field.value}
            >
              <FloatLayout adaptiveWidth>
                <ListboxButton hasError={!!fieldState.error}>
                  <FlexContainer alignItems="center" as="span">
                    <ConnectorIcon icon={destination.icon} className={styles.selectDestinationObjectName__icon} />
                    <Box py="md" as="span">
                      {field.value ? (
                        <Text size="lg">{field.value}</Text>
                      ) : (
                        <Text size="lg" color="grey">
                          <FormattedMessage id="connection.create.selectDestinationObject" />
                        </Text>
                      )}
                    </Box>
                  </FlexContainer>
                </ListboxButton>
                <ListboxOptions>
                  {destinationObjectNameOptions.map(({ label, value }, index) => {
                    return (
                      <ListboxOption value={value} key={index}>
                        {() => (
                          <Box p="md" pr="none" as="span">
                            <Text size="lg">{label}</Text>
                          </Box>
                        )}
                      </ListboxOption>
                    );
                  })}
                </ListboxOptions>
              </FloatLayout>
            </Listbox>
            {fieldState.error && <FormControlErrorMessage name={field.name} />}
          </FlexContainer>
        )}
      />
    </div>
  );
};
