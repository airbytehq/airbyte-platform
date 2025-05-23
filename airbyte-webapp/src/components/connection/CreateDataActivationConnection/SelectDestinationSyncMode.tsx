import { Listbox } from "@headlessui/react";
import React from "react";
import { Controller, useFormContext } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Option } from "components/ui/ListBox";
import { FloatLayout } from "components/ui/ListBox/FloatLayout";
import { ListboxButton } from "components/ui/ListBox/ListboxButton";
import { ListboxOption } from "components/ui/ListBox/ListboxOption";
import { ListboxOptions } from "components/ui/ListBox/ListboxOptions";
import { Text } from "components/ui/Text";

import { DestinationSyncMode } from "core/api/types/AirbyteClient";

import { StreamMappingsFormValues } from "./StreamMappings";

interface SelectDestinationSyncModeProps {
  streamIndex: number;
}

export const SelectDestinationSyncMode: React.FC<SelectDestinationSyncModeProps> = ({ streamIndex }) => {
  const { control, setValue } = useFormContext<StreamMappingsFormValues>();
  const { formatMessage } = useIntl();

  // TODO: Update this to support update and soft_delete once they're available https://github.com/airbytehq/airbyte-internal-issues/issues/12920
  const destinationSyncModeOptions: Array<Option<DestinationSyncMode>> = [
    { label: formatMessage({ id: "connection.dataActivation.append" }), value: DestinationSyncMode.append },
    {
      label: formatMessage({ id: "connection.dataActivation.append_dedup" }),
      value: DestinationSyncMode.append_dedup,
    },
  ];

  return (
    <Controller
      name={`streams.${streamIndex}.destinationSyncMode`}
      control={control}
      render={({ field }) => (
        <Listbox
          value={field.value}
          onChange={(value) => {
            if (value === DestinationSyncMode.append_dedup) {
              setValue(`streams.${streamIndex}.primaryKey`, undefined);
            }
            field.onChange(value);
          }}
        >
          <FloatLayout adaptiveWidth>
            <ListboxButton>
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
                <ListboxOption
                  key={option.value}
                  value={option.value}
                  onClick={() => {
                    if (option.value === "append_dedup") {
                      setValue(`streams.${streamIndex}.primaryKey`, undefined);
                    }
                    field.onChange(option.value);
                  }}
                >
                  <Box p="md" pr="none" as="span">
                    <Text>{option.label}</Text>
                  </Box>
                </ListboxOption>
              ))}
            </ListboxOptions>
          </FloatLayout>
        </Listbox>
      )}
    />
  );
};
