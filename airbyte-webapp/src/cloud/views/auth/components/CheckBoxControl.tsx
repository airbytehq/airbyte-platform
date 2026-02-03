import React from "react";

import { CheckBox } from "components/ui/CheckBox";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Text } from "components/ui/Text";

interface CheckBoxControlProps extends React.InputHTMLAttributes<HTMLInputElement> {
  label?: React.ReactNode;
}

export const CheckBoxControl: React.FC<CheckBoxControlProps> = (props) => (
  <FlexContainer direction="row" alignItems="center">
    <FlexItem grow={false}>
      <CheckBox {...props} id={`checkbox-${props.name}`} />
    </FlexItem>
    <label htmlFor={`checkbox-${props.name}`}>
      <Text size="sm" color={props.disabled ? "grey300" : "darkBlue"}>
        {props.label}
      </Text>
    </label>
  </FlexContainer>
);
