import { Icon } from "components/ui/Icon";
import { ListBox, ListBoxControlButtonProps, ListBoxProps } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import styles from "./InnerListBox.module.scss";

const ControlButton = <T,>(props: ListBoxControlButtonProps<T>) => {
  return (
    <>
      {props.selectedOption && <Text size="md">{props.selectedOption.label}</Text>}
      <Icon type="chevronDown" className={styles.arrow} />
    </>
  );
};

type InnerListBoxProps<T> = Omit<ListBoxProps<T>, "controlButton" | "buttonClassName">;

export const InnerListBox = <T,>(props: InnerListBoxProps<T>) => {
  return <ListBox {...props} buttonClassName={styles.button} controlButton={ControlButton} />;
};
