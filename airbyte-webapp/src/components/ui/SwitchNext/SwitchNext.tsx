import { Switch } from "@headlessui/react";
import classNames from "classnames";
import { motion } from "framer-motion";
import React from "react";
import { useIntl } from "react-intl";

import styles from "./SwitchNext.module.scss";
import { Text } from "../Text";

export interface SwitchNextProps {
  checked: boolean;
  disabled?: boolean;
  loading?: boolean;
  onChange: (checked: boolean) => void;
  name?: string;
  checkedText?: string;
  uncheckedText?: string;
  className?: string;
  testId?: string;
}

export const SwitchNext: React.FC<SwitchNextProps> = (props) => {
  const { formatMessage } = useIntl();

  const {
    name,
    checked,
    disabled,
    loading,
    onChange,
    checkedText = formatMessage({ id: "ui.switch.enabled" }),
    uncheckedText = formatMessage({ id: "ui.switch.disabled" }),
    testId,
    className,
  } = props;

  return (
    <Switch
      name={name}
      checked={checked}
      onChange={onChange}
      className={classNames(
        styles.button,
        {
          [styles.checked]: checked,
          [styles.loading]: loading,
        },
        className
      )}
      disabled={loading || disabled}
      data-testid={testId}
    >
      <motion.span layout className={styles.knob} />
      <span
        className={classNames(styles.stripe, {
          [styles.loading]: loading,
          [styles.reverse]: checked,
        })}
      />
      <Text size="xs" className={classNames(styles.text, { [styles.checkedText]: checked })}>
        {checked ? checkedText : uncheckedText}
      </Text>
    </Switch>
  );
};
