import classNames from "classnames";

import { Icon } from "components/ui/Icon";
import { ListBox, ListBoxControlButtonProps, ListBoxProps } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import styles from "./InnerListBox.module.scss";

const ControlButton = <T,>(props: ListBoxControlButtonProps<T>) => {
  return (
    <>
      {props.selectedOption && (
        <Text size="md" className={styles.buttonText}>
          {props.selectedOption.label}
        </Text>
      )}
      <Icon type="chevronDown" className={styles.arrow} />
    </>
  );
};

type InnerListBoxProps<T> = Omit<ListBoxProps<T>, "controlButton">;

export const InnerListBox = <T,>({ buttonClassName, ...restProps }: InnerListBoxProps<T>) => {
  return (
    <ListBox
      buttonClassName={classNames(styles.button, buttonClassName)}
      {...restProps}
      controlButton={ControlButton}
    />
  );
};
