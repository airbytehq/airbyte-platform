import classNames from "classnames";
import omit from "lodash/omit";
import React from "react";

import { CheckBox } from "components/ui/CheckBox";
import { Switch } from "components/ui/Switch";

import styles from "./LabeledSwitch.module.scss";

interface LabeledSwitchProps extends Omit<React.InputHTMLAttributes<HTMLInputElement>, "size"> {
  message?: React.ReactNode;
  label?: React.ReactNode;
  checkbox?: boolean;
  loading?: boolean;
  id?: string;
}

export const LabeledSwitch = React.forwardRef<HTMLDivElement, LabeledSwitchProps>((props, ref) => {
  const switchId = props.id ?? `toggle-${props.name}`;

  return (
    <div ref={ref} className={classNames(styles.labeledSwitch, props.className)}>
      <span>
        {props.checkbox ? <CheckBox {...omit(props, "checkbox")} id={switchId} /> : <Switch {...props} id={switchId} />}
      </span>

      <label
        className={classNames(styles.label, {
          [styles.disabled]: props.disabled,
        })}
        htmlFor={switchId}
      >
        {props.label}
        <span className={styles.additionalMessage}>{props.message}</span>
      </label>
    </div>
  );
});
LabeledSwitch.displayName = "LabeledSwitch";
