import classNames from "classnames";
import React from "react";

import { Box } from "components/ui/Box";

import styles from "./PlanCard.module.scss";

interface PlanCardProps {
  variant: "primary" | "clear" | "purple";
}

export const PlanCard: React.FC<React.PropsWithChildren<PlanCardProps>> = ({ variant, children }) => {
  return (
    <Box
      px="xl"
      py="lg"
      className={classNames(styles.planCard, {
        [styles["planCard--primary"]]: variant === "primary",
        [styles["planCard--purple"]]: variant === "purple",
      })}
    >
      {children}
    </Box>
  );
};
