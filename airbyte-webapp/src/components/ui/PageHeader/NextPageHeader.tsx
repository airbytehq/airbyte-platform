import classNames from "classnames";
import React from "react";

import styles from "./NextPageHeader.module.scss";
import { FlexContainer } from "../Flex";

interface NextPageHeaderProps {
  leftComponent: string | React.ReactNode;
  endComponent?: React.ReactNode;
}
export const NextPageHeader: React.FC<NextPageHeaderProps> = ({ leftComponent, endComponent }) => (
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
