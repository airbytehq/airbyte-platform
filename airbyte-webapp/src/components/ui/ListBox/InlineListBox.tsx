import React from "react";
import { useIntl } from "react-intl";

import { Text } from "components/ui/Text";

import styles from "./InlineListBox.module.scss";
import { ListBox, ListBoxControlButtonProps, ListBoxProps } from "./ListBox";
import { Button } from "../Button";

type InlineListBoxProps<T> = Omit<ListBoxProps<T>, "controlButton" | "buttonClassName" | "optionClassName">;

export const InlineListBox = <T,>({ ...rest }: InlineListBoxProps<T>) => {
  const { formatMessage } = useIntl();

  const ControlButton: React.FC = ({ selectedOption, isDisabled }: ListBoxControlButtonProps<unknown>) => (
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
    <ListBox<T>
      {...(rest as ListBoxProps<T>)}
      controlButton={ControlButton}
      buttonClassName={styles.controlButton}
      optionClassName={styles.option}
    />
  );
};
