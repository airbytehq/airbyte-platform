import React from "react";
import styled from "styled-components";

import { CheckBox } from "components/ui/CheckBox";
import { FlexItem } from "components/ui/Flex";

type IProps = {
  label?: React.ReactNode;
} & React.InputHTMLAttributes<HTMLInputElement>;

const ToggleContainer = styled.div`
  display: flex;
  flex-direction: row;
  align-items: center;
`;

const Label = styled.label<{ disabled?: boolean }>`
  padding-left: 7px;
  font-size: 11px;
  line-height: 13px;
  color: ${({ theme }) => theme.textColor};
  cursor: pointer;
`;

const CheckBoxControl: React.FC<IProps> = (props) => (
  <ToggleContainer>
    <FlexItem grow={false}>
      <CheckBox {...props} id={`checkbox-${props.name}`} />
    </FlexItem>
    <Label disabled={props.disabled} htmlFor={`checkbox-${props.name}`}>
      {props.label}
    </Label>
  </ToggleContainer>
);

export default CheckBoxControl;
