import classNames from "classnames";

import styles from "./Badge.module.scss";

interface BadgeProps {
  className?: string;
  variant: "blue" | "grey" | "green" | "darkBlue" | "lightBlue";
  uppercase?: boolean;
  "data-testid"?: string;
}

export const Badge: React.FC<React.PropsWithChildren<BadgeProps>> = ({
  children,
  className,
  variant = "grey",
  uppercase = true,
  "data-testid": testId,
}) => {
  return (
    <span
      className={classNames(styles.badge, className, {
        [styles["badge--blue"]]: variant === "blue",
        [styles["badge--grey"]]: variant === "grey",
        [styles["badge--green"]]: variant === "green",
        [styles["badge--darkBlue"]]: variant === "darkBlue",
        [styles["badge--uppercase"]]: uppercase,
      })}
      data-testid={testId}
    >
      {children}
    </span>
  );
};
