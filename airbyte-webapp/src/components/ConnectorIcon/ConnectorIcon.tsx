import classNames from "classnames";
import React from "react";

import { FlexContainer } from "components/ui/Flex";

import { SvgIcon } from "area/connector/utils";

import styles from "./ConnectorIcon.module.scss";

interface ConnectorIconProps {
  icon?: string;
  className?: string;
}

export const ConnectorIcon: React.FC<ConnectorIconProps> = ({ className, icon }) => (
  <FlexContainer className={classNames(styles.content, className)} aria-hidden="true" alignItems="center">
    <SvgIcon src={icon} />
  </FlexContainer>
);
