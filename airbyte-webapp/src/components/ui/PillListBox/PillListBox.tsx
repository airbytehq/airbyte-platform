import { useMemo } from "react";
import { useIntl } from "react-intl";

import { PillButton, PillButtonProps } from "./PillButton";
import styles from "./PillListBox.module.scss";
import { ListBox, ListBoxControlButtonProps, ListBoxProps } from "../ListBox";

type PillListBoxProps<T> = ListBoxProps<T> & PillButtonProps;

export const PillListBox = <T,>({ active, variant, hasError, pillClassName, ...restProps }: PillListBoxProps<T>) => {
  const { formatMessage } = useIntl();

  const CustomPillButton: React.FC = useMemo(
    () =>
      // eslint-disable-next-line react/display-name
      <T,>({ selectedOption, isDisabled }: ListBoxControlButtonProps<T>) => (
        <PillButton
          active={active}
          variant={variant}
          disabled={isDisabled}
          hasError={hasError}
          pillClassName={pillClassName}
          data-testid="pill-select-button"
        >
          {selectedOption ? selectedOption.label : formatMessage({ id: "form.selectValue" })}
        </PillButton>
      ),
    [active, formatMessage, hasError, pillClassName, variant]
  );
  CustomPillButton.displayName = "CustomPillButton";

  return (
    <ListBox<T>
      controlButton={CustomPillButton}
      buttonClassName={styles.controlButton}
      optionClassName={styles.option}
      placement="bottom-start"
      {...restProps}
    />
  );
};
