import { Listbox } from "@headlessui/react";
import React, { useMemo } from "react";
import { Controller, useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { FormControlErrorMessage } from "components/forms/FormControl";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Option } from "components/ui/ListBox";
import { FloatLayout } from "components/ui/ListBox/FloatLayout";
import { ListboxButton } from "components/ui/ListBox/ListboxButton";
import { ListboxOption } from "components/ui/ListBox/ListboxOption";
import { ListboxOptions } from "components/ui/ListBox/ListboxOptions";
import { Text } from "components/ui/Text";

import { DataActivationConnectionFormValues } from "area/dataActivation/types";
import { DestinationCatalog, DestinationSyncMode } from "core/api/types/AirbyteClient";

interface SelectDestinationSyncModeProps {
  streamIndex: number;
  destinationCatalog: DestinationCatalog;
}

export const SelectDestinationSyncMode: React.FC<SelectDestinationSyncModeProps> = ({
  destinationCatalog,
  streamIndex,
}) => {
  const { control, setValue } = useFormContext<DataActivationConnectionFormValues>();
  const { formatMessage } = useIntl();

  const destinationObjectName = useWatch<DataActivationConnectionFormValues, `streams.${number}.destinationObjectName`>(
    {
      name: `streams.${streamIndex}.destinationObjectName`,
    }
  );

  const availableOperations = useMemo(() => {
    return destinationCatalog.operations.filter((operation) => operation.objectName === destinationObjectName);
  }, [destinationObjectName, destinationCatalog]);

  const destinationSyncModeOptions: Array<Option<DestinationSyncMode>> = [
    { label: formatMessage({ id: "connection.dataActivation.append" }), value: DestinationSyncMode.append },
    {
      label: formatMessage({ id: "connection.dataActivation.append_dedup" }),
      value: DestinationSyncMode.append_dedup,
    },
    {
      label: formatMessage({ id: "connection.dataActivation.update" }),
      value: DestinationSyncMode.update,
    },
    {
      label: formatMessage({ id: "connection.dataActivation.soft_delete" }),
      value: DestinationSyncMode.soft_delete,
    },
  ].map((option) => ({
    ...option,
    disabled: !availableOperations.some((operation) => operation.syncMode === option.value),
  }));

  return (
    <Controller
      name={`streams.${streamIndex}.destinationSyncMode`}
      control={control}
      render={({ field, fieldState }) => (
        <FlexContainer direction="column" gap="xs">
          <Listbox
            value={field.value}
            onChange={(value) => {
              if (value === field.value) {
                return;
              }
              if (value === DestinationSyncMode.append) {
                setValue(`streams.${streamIndex}.matchingKeys`, null);
              }
              if (value === DestinationSyncMode.append_dedup) {
                setValue(`streams.${streamIndex}.matchingKeys`, null);
              }
              field.onChange(value);
            }}
          >
            <FloatLayout adaptiveWidth>
              <ListboxButton hasError={!!fieldState.error}>
                {field.value ? (
                  <FlexContainer as="span">
                    <Text>{destinationSyncModeOptions.find((option) => option.value === field.value)?.label}</Text>
                    <Text color="grey">
                      <FormattedMessage id="connection.dataActivation.destinationSyncMode" />
                    </Text>
                  </FlexContainer>
                ) : (
                  <Text color="grey">
                    <FormattedMessage id="connection.dataActivation.selectDestinationSyncMode" />
                  </Text>
                )}
              </ListboxButton>
              <ListboxOptions>
                {destinationSyncModeOptions.map((option) => (
                  <ListboxOption key={option.value} value={option.value} disabled={option.disabled}>
                    <Box p="md" pr="none" as="span">
                      <Text>{option.label}</Text>
                    </Box>
                  </ListboxOption>
                ))}
              </ListboxOptions>
            </FloatLayout>
          </Listbox>
          {fieldState.error && <FormControlErrorMessage name={field.name} />}
        </FlexContainer>
      )}
    />
  );
};
