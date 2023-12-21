import classNames from "classnames";
import React from "react";

import styles from "./LinkTab.module.scss";
import { Link } from "../Link";
import { Text } from "../Text";

interface LinkTabProps extends LinkTabInnerProps {
  to: string;
  id: string;
}

interface LinkTabInnerProps {
  name: string | React.ReactNode;
  isActive?: boolean;
  disabled?: boolean;
}

const LinkTabInner: React.FC<LinkTabInnerProps> = ({ name, isActive, disabled = false }) => {
  return (
    <div
      className={classNames(styles.tabContainer, {
        [styles["tabContainer--active"]]: isActive,
        [styles["tabContainer--inactive"]]: !isActive,
        [styles["tabContainer--disabled"]]: disabled,
      })}
    >
      <Text color={disabled ? "grey300" : isActive ? "darkBlue" : "grey"} size="lg">
        {name}
      </Text>
    </div>
  );
};

export const LinkTab: React.FC<LinkTabProps> = ({ name, id, isActive, to, disabled = false }) => {
  return disabled ? (
    <LinkTabInner name={name} isActive={isActive} disabled={disabled} />
  ) : (
    <Link to={to} className={styles.link} data-testid={`${id.toLowerCase()}-step`}>
      <LinkTabInner name={name} isActive={isActive} disabled={disabled} />
    </Link>
  );
};
