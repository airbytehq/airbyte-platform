import React from "react";

import { Button } from "components/ui/Button";
import { ListBox, ListBoxControlButtonProps, ListBoxProps } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import styles from "./StreamsListHeaderListbox.module.scss";

type StreamsListHeaderListboxProps<T> = Omit<ListBoxProps<T>, "controlButton" | "buttonClassName" | "optionClassName">;

export const StreamsListHeaderListbox = <T,>({ ...rest }: StreamsListHeaderListboxProps<T>) => {
  const controlButtonContent = ({ selectedOption }: ListBoxControlButtonProps<T>) => (
    <Text size="xs">{selectedOption?.label}</Text>
  );

  const ControlButton = React.forwardRef<HTMLButtonElement, ListBoxControlButtonProps<T>>((props, ref) => (
    <Button {...props} type="button" variant="clear" icon="caretDown" iconPosition="right" iconSize="xs" ref={ref} />
  ));
  ControlButton.displayName = "ControlButton";

  return (
    <ListBox<T>
      {...(rest as ListBoxProps<T>)}
      controlButton={controlButtonContent}
      controlButtonAs={ControlButton}
      buttonClassName={styles.controlButton}
    />
  );
};
