import classnames from "classnames";
import React, { ComponentProps, ReactElement } from "react";

import { ControlLabels } from "components/LabeledControl";
import { FlexContainer } from "components/ui/Flex";

import styles from "./FormFieldLayout.module.scss";

interface FormFieldLayoutProps {
  children: [ReactElement<typeof ControlLabels>, ...React.ReactNode[]];
  alignItems?: ComponentProps<typeof FlexContainer>["alignItems"];
  nextSizing?: boolean;
}

export const FormFieldLayout: React.FC<FormFieldLayoutProps> = ({
  children,
  alignItems = "center",
  nextSizing = false,
}) => {
  const [label, ...restControls] = React.Children.toArray(children);

  return (
    <FlexContainer alignItems={alignItems} gap="xl">
      <div className={classnames(styles.leftFieldCol, { [styles.nextSizing]: nextSizing })}>{label}</div>
      <div className={classnames(styles.rightFieldCol, { [styles.nextSizing]: nextSizing })}>{restControls}</div>
    </FlexContainer>
  );
};
