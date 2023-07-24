import classNames from "classnames";
import React from "react";

import { FlexContainer } from "components/ui/Flex";
import { RadioButton } from "components/ui/RadioButton";

import styles from "./LabeledRadioButton.module.scss";
type IProps = {
  message?: React.ReactNode;
  label?: React.ReactNode;
  className?: string;
} & React.InputHTMLAttributes<HTMLInputElement>;

const LabeledRadioButton: React.FC<IProps> = (props) => (
  <FlexContainer className={classNames(props.className, styles.container)} alignItems="center" gap="none">
    <RadioButton {...props} id={`radiobutton-${props.id || props.name}`} disabled={props.disabled} />
    <label
      className={classNames(styles.label, { [styles.disabled]: props.disabled })}
      htmlFor={`radiobutton-${props.id || props.name}`}
    >
      {props.label}
      <span className={styles.message}>{props.message}</span>
    </label>
  </FlexContainer>
);

export default LabeledRadioButton;
