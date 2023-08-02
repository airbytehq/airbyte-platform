import classNames from "classnames";
import React from "react";

import { FlexContainer } from "components/ui/Flex";
import { RadioButton } from "components/ui/RadioButton";

import styles from "./LabeledRadioButton.module.scss";

export interface LabeledRadioButtonProps extends React.InputHTMLAttributes<HTMLInputElement> {
  message?: React.ReactNode;
  label?: React.ReactNode;
}

export const LabeledRadioButton = React.forwardRef<HTMLDivElement, LabeledRadioButtonProps>((props, ref) => (
  <FlexContainer ref={ref} alignItems="center" gap="sm" className={classNames(styles.container, props.className)}>
    <RadioButton {...props} id={`radiobutton-${props.id || props.name}`} disabled={props.disabled} />
    <label
      className={classNames(styles.label, { [styles["label--disabled"]]: props.disabled })}
      htmlFor={`radiobutton-${props.id || props.name}`}
    >
      {props.label}
      <span className={styles.message}>{props.message}</span>
    </label>
  </FlexContainer>
));

LabeledRadioButton.displayName = "LabeledRadioButton";
