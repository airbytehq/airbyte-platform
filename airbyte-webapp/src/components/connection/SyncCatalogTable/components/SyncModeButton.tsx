import React, { useMemo } from "react";
import { useIntl } from "react-intl";

import { CatalogListBox } from "components/connection/SyncCatalogTable/components/CatalogListBox/CatalogListBox";
import { Option } from "components/ui/ListBox";

import { SyncModeValue } from "./SyncModeCell";

interface SyncModeSelectProps {
  onChange: (option: SyncModeValue) => void;
  options: SyncModeValue[];
  value: SyncModeValue | undefined;
  disabled?: boolean;
}

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
    <CatalogListBox<SyncModeValue>
      {...restProps}
      isDisabled={disabled}
      options={syncModeOptions}
      selectedValue={value}
      onSelect={onChange}
      placement="bottom-start"
    />
  );
};
