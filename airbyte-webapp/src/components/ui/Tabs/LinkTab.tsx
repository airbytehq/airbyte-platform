import classNames from "classnames";
import React from "react";

import styles from "./LinkTab.module.scss";
import { Link } from "../Link";
import { Text } from "../Text";

interface LinkTabProps extends LinkTabInnerProps {
  to: string;
}

interface LinkTabInnerProps {
  id: string;
  name: string | React.ReactNode;
  isActive?: boolean;
  disabled?: boolean;
}

const LinkTabInner: React.FC<LinkTabInnerProps> = ({ name, id, isActive, disabled = false }) => {
  return (
    <div
      data-id={`${id.toLowerCase()}-step`}
      className={classNames(styles.tabContainer, {
        [styles["tabContainer--active"]]: isActive,
        [styles["tabContainer--inactive"]]: !isActive,
        [styles["tabContainer--disabled"]]: disabled,
      })}
    >
      <Text color={disabled ? "grey300" : isActive ? "darkBlue" : "grey"} className={styles.text} size="lg">
        {name}
      </Text>
    </div>
  );
};

export const LinkTab: React.FC<LinkTabProps> = ({ name, id, isActive, to, disabled = false }) => {
  return disabled ? (
    <LinkTabInner name={name} id={id} isActive={isActive} disabled={disabled} />
  ) : (
    <Link to={to} className={styles.link}>
      <LinkTabInner name={name} id={id} isActive={isActive} disabled={disabled} />
    </Link>
  );
};
