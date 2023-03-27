import classNames from "classnames";
import { useMemo } from "react";
import { FormattedMessage } from "react-intl";

import { DropDownOptionDataItem } from "components/ui/DropDown";
import { PillSelect, PillButtonVariant } from "components/ui/PillSelect";

import { DestinationSyncMode, SyncMode } from "core/request/AirbyteClient";

import styles from "./SyncModeSelect.module.scss";

export interface SyncModeValue {
  syncMode: SyncMode;
  destinationSyncMode: DestinationSyncMode;
}

export interface SyncModeOption {
  value: SyncModeValue;
}

interface SyncModeSelectProps {
  className?: string;
  onChange?: (option: DropDownOptionDataItem<SyncModeValue>) => void;
  options: SyncModeOption[];
  value: Partial<SyncModeValue>;
  variant?: PillButtonVariant;
  disabled?: boolean;
}

export const SyncModeSelect: React.FC<SyncModeSelectProps> = ({
  className,
  options,
  onChange,
  value,
  variant,
  disabled,
}) => {
  const pillSelectOptions = useMemo(() => {
    return options.map(({ value }) => {
      const { syncMode, destinationSyncMode } = value;
      return {
        label: [
          <FormattedMessage key={`syncMode.${syncMode}`} id={`syncMode.${syncMode}`} />,
          <FormattedMessage
            key={`destinationSyncMode.${destinationSyncMode}`}
            id={`destinationSyncMode.${destinationSyncMode}`}
          />,
        ],
        value,
      };
    });
  }, [options]);

  return (
    <PillSelect
      options={pillSelectOptions}
      value={value}
      onChange={onChange}
      className={classNames(styles.pillSelect, className)}
      variant={variant}
      disabled={disabled}
      data-testid="sync-mode-select"
    />
  );
};
