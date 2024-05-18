import React, { useMemo } from "react";
import { useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { ListBox, ListBoxControlButtonProps, Option } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import styles from "./SyncModeButton.module.scss";
import { SyncModeSelectProps, SyncModeValue } from "../../../syncCatalog/SyncModeSelect";

export const SyncModeButton: React.FC<SyncModeSelectProps> = ({ options, value, onChange, disabled }) => {
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

  const ControlButton: React.FC = ({ selectedOption, isDisabled }: ListBoxControlButtonProps<SyncModeValue>) => (
    <Button
      type="button"
      variant="clear"
      disabled={isDisabled}
      icon="caretDown"
      iconPosition="right"
      className={styles.button}
    >
      <Text color="grey400">{selectedOption ? selectedOption.label : formatMessage({ id: "form.selectValue" })}</Text>
    </Button>
  );

  return (
    <ListBox<SyncModeValue>
      isDisabled={disabled}
      options={syncModeOptions}
      selectedValue={value}
      onSelect={onChange}
      controlButton={ControlButton}
      buttonClassName={styles.controlButton}
      optionClassName={styles.option}
      placement="bottom-start"
    />
  );
};
