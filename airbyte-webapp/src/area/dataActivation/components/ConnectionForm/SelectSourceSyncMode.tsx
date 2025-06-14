import { Listbox } from "@headlessui/react";
import React from "react";
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
import { AirbyteCatalog, SyncMode } from "core/api/types/AirbyteClient";

interface SelectSourceSyncModeProps {
  streamIndex: number;
  sourceCatalog: AirbyteCatalog;
}

const DATA_ACTIVATION_SUPPORTED_SYNC_MODES = [SyncMode.incremental, SyncMode.full_refresh];

export const SelectSourceSyncMode: React.FC<SelectSourceSyncModeProps> = ({ streamIndex, sourceCatalog }) => {
  const sourceStreamDescriptor = useWatch<
    DataActivationConnectionFormValues,
    `streams.${number}.sourceStreamDescriptor`
  >({
    name: `streams.${streamIndex}.sourceStreamDescriptor`,
  });
  const { control, setValue } = useFormContext<DataActivationConnectionFormValues>();
  const { formatMessage } = useIntl();

  const sourceStreamSupportedSyncModes: SyncMode[] =
    sourceCatalog.streams.find(
      (stream) =>
        stream.stream?.name === sourceStreamDescriptor?.name &&
        stream.stream?.namespace === sourceStreamDescriptor?.namespace
    )?.stream?.supportedSyncModes ?? [];

  const sourceSyncModeOptions: Array<Option<SyncMode>> = DATA_ACTIVATION_SUPPORTED_SYNC_MODES.map((mode) => ({
    label: formatMessage({ id: `syncMode.${mode}` }),
    value: mode,
    disabled: !sourceStreamSupportedSyncModes.includes(mode),
  }));

  return (
    <Controller
      name={`streams.${streamIndex}.sourceSyncMode`}
      control={control}
      render={({ field, fieldState }) => (
        <FlexContainer direction="column" gap="xs">
          <Listbox
            value={field.value}
            onChange={(value) => {
              if (value === field.value) {
                return;
              }
              if (value === SyncMode.incremental) {
                setValue(`streams.${streamIndex}.cursorField`, "");
              }
              if (value === SyncMode.full_refresh) {
                setValue(`streams.${streamIndex}.cursorField`, null);
              }
              field.onChange(value);
            }}
          >
            <FloatLayout adaptiveWidth>
              <ListboxButton hasError={!!fieldState.error}>
                {field.value ? (
                  <FlexContainer as="span">
                    <Text>{sourceSyncModeOptions.find((option) => option.value === field.value)?.label}</Text>
                    <Text color="grey">
                      <FormattedMessage id="connection.syncMode" />
                    </Text>
                  </FlexContainer>
                ) : (
                  <Text color="grey">
                    <FormattedMessage id="connection.selectSyncMode" />
                  </Text>
                )}
              </ListboxButton>
              <ListboxOptions>
                {sourceSyncModeOptions.map((option) => (
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
