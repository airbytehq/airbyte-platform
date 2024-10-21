import classNames from "classnames";
import React from "react";

import styles from "./PageHeader.module.scss";
import { FlexContainer } from "../Flex";

interface PageHeaderProps {
  leftComponent: string | React.ReactNode;
  endComponent?: React.ReactNode;
  className?: string;
}
export const PageHeader: React.FC<PageHeaderProps> = ({ leftComponent, endComponent, className }) => (
  <FlexContainer
    className={classNames(styles.container, className)}
    alignItems="center"
    justifyContent="space-between"
    data-testid="page-header-container"
  >
    {leftComponent}
    {endComponent}
  </FlexContainer>
);
