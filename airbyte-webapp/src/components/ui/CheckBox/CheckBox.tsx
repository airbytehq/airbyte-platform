import classNames from "classnames";
import React, { InputHTMLAttributes } from "react";

import { Icon } from "components/ui/Icon";

import styles from "./CheckBox.module.scss";

type CheckBoxSize = "lg" | "sm";

export interface CheckBoxProps extends InputHTMLAttributes<HTMLInputElement> {
  indeterminate?: boolean;
  checkboxSize?: CheckBoxSize;
}

export const CheckBox: React.FC<CheckBoxProps> = ({ indeterminate, checkboxSize = "lg", ...inputProps }) => {
  const { checked, disabled, className } = inputProps;

  const checkMarkSize = checkboxSize === "lg" ? "md" : "sm";

  // Without this, two click events will bubble due to how the input is nested. This breaks headless UI's change
  // detection, so we stop the duplicate event from bubbling.
  const handleClick = (e: React.MouseEvent<HTMLInputElement, MouseEvent>) => {
    e.stopPropagation();
  };

  return (
    <label
      className={classNames(
        styles.container,
        {
          [styles.checked]: checked,
          [styles.indeterminate]: indeterminate,
          [styles.disabled]: disabled,
          [styles.sizeLg]: checkboxSize === "lg",
          [styles.sizeSm]: checkboxSize === "sm",
        },
        className
      )}
    >
      <input type="checkbox" aria-checked={indeterminate ? "mixed" : checked} {...inputProps} onClick={handleClick} />

      {indeterminate ? (
        <Icon type="minus" size={checkMarkSize} />
      ) : (
        checked && <Icon type="check" size={checkMarkSize} />
      )}
    </label>
  );
};
