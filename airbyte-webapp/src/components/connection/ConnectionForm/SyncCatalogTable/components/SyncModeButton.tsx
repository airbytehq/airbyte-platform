import React, { useMemo } from "react";
import { useIntl } from "react-intl";

import { Option } from "components/ui/ListBox";
import { InlineListBox } from "components/ui/ListBox/InlineListBox";

import { SyncModeSelectProps, SyncModeValue } from "../../../syncCatalog/SyncModeSelect";

export const SyncModeButton: React.FC<SyncModeSelectProps> = ({ options, value, onChange, disabled, ...restProps }) => {
  const { formatMessage } = useIntl();

  const syncModeOptions: Array<Option<SyncModeValue>> = useMemo(
    () =>
      options.map((option) => ({
        label: `${formatMessage({ id: `syncMode.${option.syncMode}` })} | ${formatMessage({
          id: `destinationSyncMode.${option.destinationSyncMode}`,
        })}`,
        value: option,
      })),
    [formatMessage, options]
  );

  return (
    <InlineListBox<SyncModeValue>
      {...restProps}
      isDisabled={disabled}
      options={syncModeOptions}
      selectedValue={value}
      onSelect={onChange}
      placement="bottom-start"
    />
  );
};
