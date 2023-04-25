import classNames from "classnames";
import React from "react";

import styles from "./LinkTab.module.scss";
import { Box } from "../Box";
import { Link } from "../Link";
import { Text } from "../Text";

interface LinkTabProps {
  id: string;
  name: string | React.ReactNode;
  to: string;
  isActive?: boolean;
}

export const LinkTab: React.FC<LinkTabProps> = ({ name, id, isActive, to }) => {
  return (
    <Link to={to} className={styles.link}>
      <Box
        py="lg"
        data-id={`${id.toLowerCase()}-step`}
        className={classNames(styles.tabContainer, {
          [styles["tabContainer--active"]]: isActive,
          [styles["tabContainer--inactive"]]: !isActive,
        })}
      >
        <Text color={isActive ? "darkBlue" : "grey"} className={styles.text} size="lg">
          {name}
        </Text>
      </Box>
    </Link>
  );
};
