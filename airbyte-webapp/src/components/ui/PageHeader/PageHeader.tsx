import classNames from "classnames";
import React from "react";

import styles from "./PageHeader.module.scss";
import { FlexContainer } from "../Flex";

interface PageHeaderProps {
  leftComponent: string | React.ReactNode;
  endComponent?: React.ReactNode;
}
export const PageHeader: React.FC<PageHeaderProps> = ({ leftComponent, endComponent }) => (
  <FlexContainer
    className={classNames(styles.container)}
    alignItems="center"
    justifyContent="space-between"
    data-testid="page-header-container"
  >
    {leftComponent}
    {endComponent}
  </FlexContainer>
);
