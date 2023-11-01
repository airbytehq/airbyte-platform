import classNames from "classnames";

import styles from "./Badge.module.scss";

interface BadgeProps {
  variant: "blue" | "gray" | "green";
}

export const Badge: React.FC<React.PropsWithChildren<BadgeProps>> = ({ children, variant = "gray" }) => {
  return (
    <span
      className={classNames(styles.badge, {
        [styles["badge--blue"]]: variant === "blue",
        [styles["badge--gray"]]: variant === "gray",
        [styles["badge--green"]]: variant === "green",
      })}
    >
      {children}
    </span>
  );
};
