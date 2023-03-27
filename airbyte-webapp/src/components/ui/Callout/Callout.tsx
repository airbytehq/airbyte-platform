import classNames from "classnames";

import styles from "./Callout.module.scss";

export type CalloutVariant = "default" | "error" | "info" | "boldInfo" | "actionRequired";

interface CalloutProps {
  className?: string;
  variant?: CalloutVariant;
}

export const Callout: React.FC<React.PropsWithChildren<CalloutProps>> = ({
  children,
  className,
  variant = "default",
}) => {
  const containerStyles = classNames(styles.container, className, {
    [styles.default]: variant === "default",
    [styles.error]: variant === "error",
    [styles.info]: variant === "info",
    [styles.boldInfo]: variant === "boldInfo",
    [styles.actionRequired]: variant === "actionRequired",
  });

  return <div className={containerStyles}>{children}</div>;
};
