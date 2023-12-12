import classNames from "classnames";

import styles from "./Badge.module.scss";

interface BadgeProps {
  variant: "blue" | "grey" | "green";
}

export const Badge: React.FC<React.PropsWithChildren<BadgeProps>> = ({ children, variant = "grey" }) => {
  return (
    <span
      className={classNames(styles.badge, {
        [styles["badge--blue"]]: variant === "blue",
        [styles["badge--grey"]]: variant === "grey",
        [styles["badge--green"]]: variant === "green",
      })}
    >
      {children}
    </span>
  );
};
