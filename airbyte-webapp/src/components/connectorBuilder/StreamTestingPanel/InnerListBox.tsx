import { faAngleDown } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";

import { ListBox, ListBoxControlButtonProps, ListBoxProps } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import styles from "./InnerListBox.module.scss";

const ControlButton = <T,>(props: ListBoxControlButtonProps<T>) => {
  return (
    <>
      {props.selectedOption && <Text size="md">{props.selectedOption.label}</Text>}
      <FontAwesomeIcon className={styles.arrow} icon={faAngleDown} />
    </>
  );
};

type InnerListBoxProps<T> = Omit<ListBoxProps<T>, "controlButton" | "buttonClassName">;

export const InnerListBox = <T,>(props: InnerListBoxProps<T>) => {
  return <ListBox {...props} buttonClassName={styles.button} controlButton={ControlButton} />;
};
