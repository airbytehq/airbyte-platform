import classNames from "classnames";
import React from "react";

import styles from "./NextPageHeader.module.scss";
import { FlexContainer } from "../Flex";

interface PageHeaderProps {
  leftComponent: string | React.ReactNode;
  endComponent?: React.ReactNode;
}
// todo: things aren't lined up right when there is scrollbar in the area below the header (because the header isn't in the scrollable div)
export const NextPageHeader: React.FC<PageHeaderProps> = ({ leftComponent, endComponent }) => (
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
