import React from "react";
import { useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { ListBox, ListBoxControlButtonProps, ListBoxProps } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import styles from "./CatalogListBox.module.scss";

type CatalogListBoxProps<T> = Omit<ListBoxProps<T>, "controlButton" | "buttonClassName" | "optionClassName">;

export const CatalogListBox = <T,>({ ...rest }: CatalogListBoxProps<T>) => {
  const { formatMessage } = useIntl();

  const controlButtonContent = ({ selectedOption, isDisabled }: ListBoxControlButtonProps<T>) => (
    <Text color={isDisabled ? "grey300" : "grey400"}>
      {selectedOption ? selectedOption.label : formatMessage({ id: "form.selectValue" })}
    </Text>
  );

  const ControlButton = React.forwardRef<HTMLButtonElement, ListBoxControlButtonProps<T>>((props, ref) => (
    <Button type="button" variant="clear" icon="caretDown" iconPosition="right" ref={ref} {...props} />
  ));
  ControlButton.displayName = "ControlButton";

  return (
    <ListBox<T>
      {...(rest as ListBoxProps<T>)}
      controlButtonAs={ControlButton}
      controlButton={controlButtonContent}
      buttonClassName={styles.button}
      optionClassName={styles.option}
    />
  );
};
