import React, {useContext} from "react";
import { FormattedMessage } from "react-intl";
import styled from "styled-components";
import { LabeledSwitch } from "components/LabeledSwitch";
import {darkModeContext} from "../../../../../App";


export interface AppearanceFormProps {
  onChange: () => void;
}

const FormItem = styled.div`
  display: flex;
  flex-direction: row;
  align-items: center;
  min-height: 33px;
  margin-bottom: 10px;
`;

const AppearanceMode: React.FC<AppearanceFormProps> = ({
  onChange
}) => {

    const { inDarkMode } = useContext(darkModeContext);

  return (
    <FormItem>
      <LabeledSwitch
          checked={inDarkMode}
          disabled={false}
        label={<FormattedMessage id="preferences.useDarkMode" />}
        onChange={onChange}
      />
    </FormItem>
  );
};

export default AppearanceMode;
