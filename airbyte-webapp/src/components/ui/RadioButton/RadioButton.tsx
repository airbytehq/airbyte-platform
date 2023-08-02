import classNames from "classnames";
import React from "react";

import styles from "./RadioButton.module.scss";

export const RadioButton: React.FC<React.InputHTMLAttributes<HTMLInputElement>> = (props) => {
  return (
    <label
      className={classNames(props.className, styles.container, {
        [styles.checked]: props.checked,
        [styles.disabled]: props.disabled,
      })}
      htmlFor={props.id}
    >
      <input {...props} type="radio" className={styles.input} />
    </label>
  );
};
