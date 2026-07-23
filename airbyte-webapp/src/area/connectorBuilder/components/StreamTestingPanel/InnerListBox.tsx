import classNames from "classnames";

import { ListBox, ListBoxControlButtonProps, ListBoxProps } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import styles from "./InnerListBox.module.scss";

const ControlButton = <T,>(props: ListBoxControlButtonProps<T>) => (
  <Text size="md" className={styles.buttonText}>
    {props.selectedOption?.label ?? ""}
  </Text>
);

type InnerListBoxProps<T> = Omit<ListBoxProps<T>, "controlButton">;

export const InnerListBox = <T,>({ buttonClassName, ...restProps }: InnerListBoxProps<T>) => {
  return (
    <ListBox
      buttonClassName={classNames(styles.button, buttonClassName)}
      {...restProps}
      controlButtonContent={ControlButton}
    />
  );
};
