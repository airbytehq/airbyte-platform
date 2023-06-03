import React from "react";
import { FormattedMessage } from "react-intl";
import styled from "styled-components";

import { LabeledSwitch } from "components/LabeledSwitch";

import FeedbackBlock from "../../../components/FeedbackBlock";

export interface MetricsFormProps {
  onChange: (data: { anonymousDataCollection: boolean }) => void;
  anonymousDataCollection?: boolean;
  successMessage?: React.ReactNode;
  errorMessage?: React.ReactNode;
  isLoading?: boolean;
}

const FormItem = styled.div`
  display: flex;
  flex-direction: row;
  align-items: center;
  min-height: 33px;
  margin-bottom: 10px;
`;

const AppearanceMode: React.FC<MetricsFormProps> = ({
  onChange,
  anonymousDataCollection,
  successMessage,
  errorMessage,
  isLoading,
}) => {
  return (
    <FormItem>
      <LabeledSwitch
        checked={anonymousDataCollection}
        disabled={isLoading}
        label={<FormattedMessage id="preferences.useDarkMode" />}
        onChange={(event) => {
          onChange({ anonymousDataCollection: event.target.checked });
        }}
        loading={isLoading}
      />
      <FeedbackBlock errorMessage={errorMessage} successMessage={successMessage} isLoading={isLoading} />
    </FormItem>
  );
};

export default AppearanceMode;
