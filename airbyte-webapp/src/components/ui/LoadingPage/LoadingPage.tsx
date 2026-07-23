import classNames from "classnames";
import React from "react";
import { useIntl } from "react-intl";

import { FlexContainer } from "components/ui/Flex";

import styles from "./LoadingPage.module.scss";
import { LogoAnimation } from "./LogoAnimation";

export const LoadingPage: React.FC<{ className?: string }> = ({ className }) => {
  const { formatMessage } = useIntl();
  return (
    <FlexContainer alignItems="center" justifyContent="center" className={classNames(styles.loadingPage, className)}>
      <LogoAnimation title={formatMessage({ id: "ui.loading" })} />
    </FlexContainer>
  );
};
