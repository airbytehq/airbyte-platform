import classNames from "classnames";
import React, { PropsWithChildren } from "react";

import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import styles from "./PillButton.module.scss";

export type PillButtonVariant = "grey" | "blue" | "green" | "red" | "strong-red" | "strong-blue";

const STYLES_BY_VARIANT: Readonly<Record<PillButtonVariant, string>> = {
  grey: styles.grey,
  blue: styles.blue,
  green: styles.green,
  red: styles.red,
  "strong-red": styles.strongRed,
  "strong-blue": styles.strongBlue,
};

export interface PillButtonProps {
  active?: boolean;
  disabled?: boolean;
  variant?: PillButtonVariant;
  hasError?: boolean;
  pillClassName?: string;
}

export const PillButton: React.FC<PropsWithChildren<PillButtonProps>> = ({
  children,
  active,
  variant = "grey",
  disabled,
  hasError = false,
  pillClassName,
  ...restProps
}) => {
  const buttonClassName = classNames(
    styles.button,
    {
      [styles.active]: active,
      [styles.disabled]: disabled,
    },
    STYLES_BY_VARIANT[hasError ? "strong-red" : variant],
    pillClassName
  );

  return (
    <div className={buttonClassName} data-error={hasError} {...restProps}>
      <div className={styles.labelContainer}>
        <Text size="xs" className={styles.text}>
          {children}
        </Text>
      </div>
      <Icon type="caretDown" className={styles.icon} />
    </div>
  );
};
