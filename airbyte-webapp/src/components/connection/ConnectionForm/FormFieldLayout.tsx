import React, { ReactElement } from "react";

import { ControlLabels } from "components/LabeledControl";
import { FlexContainer } from "components/ui/Flex";

import styles from "./FormFieldLayout.module.scss";

interface FormFieldLayoutProps {
  children: [ReactElement<typeof ControlLabels>, ...React.ReactNode[]];
}

export const FormFieldLayout: React.FC<FormFieldLayoutProps> = ({ children }) => {
  const [label, ...restControls] = React.Children.toArray(children);

  return (
    <FlexContainer alignItems="center" gap="xl">
      <div className={styles.leftFieldCol}>{label}</div>
      <div className={styles.rightFieldCol}>{restControls}</div>
    </FlexContainer>
  );
};
