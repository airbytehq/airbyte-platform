import React from "react";
import { FormattedMessage } from "react-intl";
import styled from "styled-components";

import Label from "components/Label";
import { LabeledSwitch } from "components/LabeledSwitch";

import { links } from "utils/links";

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

const DocsLink = styled.a`
  text-decoration: none;
  color: ${({ theme }) => theme.primaryColor};
  cursor: pointer;
`;

const Subtitle = styled(Label)`
  padding-bottom: 9px;
`;

const Text = styled.div`
  font-style: normal;
  font-weight: normal;
  font-size: 13px;
  line-height: 150%;
  padding-bottom: 9px;
`;

const AppearanceMode: React.FC<MetricsFormProps> = ({
  onChange,
  anonymousDataCollection,
  successMessage,
  errorMessage,
  isLoading,
}) => {
  return (
    <>
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
    </>
  );
};

export default AppearanceMode;
