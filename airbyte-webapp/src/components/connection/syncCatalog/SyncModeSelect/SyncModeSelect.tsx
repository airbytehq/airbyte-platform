import { useMemo } from "react";
import { useIntl } from "react-intl";

import { Option } from "components/ui/ListBox";
import { PillButtonVariant } from "components/ui/PillListBox";
import { PillListBox } from "components/ui/PillListBox/PillListBox";

import { DestinationSyncMode, SyncMode } from "core/api/types/AirbyteClient";

import styles from "./SyncModeSelect.module.scss";

export interface SyncModeValue {
  syncMode: SyncMode;
  destinationSyncMode: DestinationSyncMode;
}

interface SyncModeSelectProps {
  onChange: (option: SyncModeValue) => void;
  options: SyncModeValue[];
  value: SyncModeValue | undefined;
  variant?: PillButtonVariant;
  disabled?: boolean;
}

export const SyncModeSelect: React.FC<SyncModeSelectProps> = ({ options, onChange, value, variant, disabled }) => {
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
    <PillListBox<SyncModeValue>
      options={syncModeOptions}
      selectedValue={value}
      onSelect={onChange}
      pillClassName={styles.pillSelect}
      variant={variant}
      disabled={disabled}
      data-testid="sync-mode-select"
    />
  );
};
